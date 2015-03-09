"""
Converts a sequence of MediaWiki Dump JSON'd revisions into diffs.  Assumes
that input to <stdin> is partitioned by page (<page.id>) and sorted in the order the
revisions were saved (ORDER BY <timestamp> ASC, <id> ASC).

Produces identical JSON with an additional 'diff' field to <stdout>.  You can
save space with `--drop-text`.

To help track down performance problems, this command also adds fields
'tokenize_time' and 'diff_time' to its output, which gives the time
(in seconds) that it took to run the tokenization and differencing
algorithms.  Since the differencing algorithm takes quadratic time, it
can take a long time on some inputs, so it is set to timeout after a
while. Use the --timeout parameter to control this timeout period (it
defaults to an hour).  If a timeout occurs, the diff_time field is set to
-1 to indicate this fact, and the 'diff' field will not be set for the
record.

To help avoid skew problems, this module also can run in mapper and
reducer modes.  Given an HDFS input file sorted in the manner
described above, a streaming map-reduce job can be run using mapper
mode for the mapper, and reducer mode for the reducer, and the output
of the job will be as expected.

Usage:
    json2diffs (-h|--help)
    json2diffs [options]
    json2diffs [options] --mapper
    json2diffs [options] --reducer

Options:
    --config=<path>    The path to difference detection configuration
    --drop-text        Drops the 'text' field from the JSON blob
    --verbose          Print out progress information
    --timeout=<secs>   How long to wait for differencing algo [default: 3600]
"""
import json
import sys
from itertools import groupby
import time
import stopit

import docopt
from deltas.detectors import Detector
from deltas.tokenizers import Tokenizer

import yamlconf

from .util import op2doc, read_docs


class Timer: # From http://preshing.com/20110924/timing-your-code-using-pythons-with-statement/
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start

def main(argv=None):
    args = docopt.docopt(__doc__, argv=argv)
    
    config_doc = yamlconf.load(open(args['--config']))
    detector = Detector.from_config(config_doc, config_doc['detector'])
    tokenizer = Tokenizer.from_config(config_doc, config_doc['tokenizer'])

    # Parse arguments here, to flag errors back to the user early, but
    # pass them down via the "args" structure
    args['drop-text'] = bool(args['--drop-text'])
    args['verbose'] = bool(args['--verbose'])
    args['mapper'] = bool(args['--mapper'])
    args['reducer'] = bool(args['--reducer'])
    args['timeout'] = int(args['--timeout'])

    run(args, read_docs(sys.stdin), detector, tokenizer)

def run(args, revision_docs, detector, tokenizer):
    drop_text = args['drop-text']

    for revision_doc in json2diffs(args, revision_docs, detector, tokenizer):
        if drop_text and 'diff' in revision_doc and 'text' in revision_doc:
            del revision_doc['text']
        
        json.dump(revision_doc, sys.stdout)
        sys.stdout.write("\n")

def diff(revision_doc, detector, last_tokens, tokens, timeout):
    with stopit.ThreadingTimeout(timeout) as ctx:
        try:
            with Timer() as t:
                operations = detector.diff(last_tokens, tokens)
        finally:
            revision_doc['diff_time'] = t.interval
    if ctx.state == ctx.TIMED_OUT: revision_doc['diff_time'] = -1
    else: revision_doc['diff'] = [op2doc(op, last_tokens, tokens)
                                  for op in operations]

def json2diffs(args, revision_docs, detector, tokenizer):
    verbose = args['verbose']
    ismapper = args['mapper']
    isreducer = args['reducer']
    timeout = args['timeout']

    page_revision_docs = groupby(revision_docs, key=lambda r:r['page']['title'])
    
    first_ever_revision_doc = True
    last_revision_doc = None
    last_revision_text = ""
    for page_title, revision_docs in page_revision_docs:
        
        if verbose: sys.stderr.write(page_title + ": ")
        
        last_tokens = []
        diff_pending = False
        first_doc_in_group = True
        for revision_doc in revision_docs:
            if verbose: sys.stderr.write("."); sys.stderr.flush()
            
            # Diff detection uses a lot of CPU.  This will be the hottest part
            # of the code.
            if not (isreducer and 'diff' in revision_doc):
                try:
                    with Timer() as t:
                        tokens = tokenizer.tokenize(revision_doc['text'] or "")
                finally:
                    revision_doc['tokenize_time'] = t.interval
            else: tokens = []

            if isreducer:
                if first_doc_in_group:
                    diff(revision_doc, detector, last_tokens, tokens, timeout)
                elif 'diff' not in revision_doc:
                    if diff_pending:
                        diff(revision_doc,detector,last_tokens,tokens,timeout)
                        diff_pending = False
                    else: diff_pending = True
            elif not (ismapper and first_ever_revision_doc):
                diff(revision_doc, detector, last_tokens, tokens, timeout)
            
            if 'text' in revision_doc:
                last_revision_text = revision_doc['text']

            if not diff_pending:
                yield revision_doc
            
            last_tokens = tokens
            first_ever_revision_doc = False
            last_revision_doc = revision_doc
            first_doc_in_group = False
        
        
        if verbose: sys.stderr.write("\n")

    if ismapper and not last_revision_doc is None:
        del last_revision_doc['diff']
        last_revision_doc['text'] = last_revision_text
        yield last_revision_doc

if __name__ == "__main__": main()
