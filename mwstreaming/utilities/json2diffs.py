"""
Converts a sequence of MediaWiki Dump JSON'd revisions into diffs.  Assumes
that input to <stdin> is partitioned by page (<page.id>) and sorted in the order the
revisions were saved (ORDER BY <timestamp> ASC, <id> ASC).

Produces identical JSON with an additional 'diff' field to <stdout>.  You can
save space with `--drop-text`.

Usage:
    json2diffs (-h|--help)
    json2diffs --config=<path> [--drop-text] [--verbose] [--mapper|--reducer]

Options:
    --config=<path>    The path to difference detection configuration
    --drop-text        Drops the 'text' field from the JSON blob
    --verbose          Print out progress information
"""
import json
import sys
from itertools import groupby

import docopt
from deltas.detectors import Detector
from deltas.tokenizers import Tokenizer

import yamlconf

from .util import op2doc, read_docs


def main(argv=None):
    args = docopt.docopt(__doc__, argv=argv)
    
    config_doc = yamlconf.load(open(args['--config']))
    detector = Detector.from_config(config_doc, config_doc['detector'])
    tokenizer = Tokenizer.from_config(config_doc, config_doc['tokenizer'])
    
    drop_text = bool(args['--drop-text'])
    verbose = bool(args['--verbose'])
    ismapper = bool(args['--mapper'])
    isreducer = bool(args['--reducer'])
    
    run(read_docs(sys.stdin), detector, tokenizer, drop_text, verbose)

def run(revision_docs, detector, tokenizer, drop_text, verbose):
    
    for revision_doc in json2diffs(revision_docs, detector, tokenizer, verbose):
        if drop_text and 'diff' in revision_doc and 'text' in revision_doc:
            del revision_doc['text']
        
        json.dump(revision_doc, sys.stdout)
        sys.stdout.write("\n")

def json2diffs(revision_docs, detector, tokenizer, verbose):
    
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
            if 'diff' not in revision_doc:
                tokens = tokenizer.tokenize(revision_doc['text'] or "")
            else: tokens = []
            last_revision_text = revision_doc['text']

            if isreducer:
                if first_doc_in_group:
                    operations = detector.diff(last_tokens, tokens)
                    revision_doc['diff'] = [op2doc(op, last_tokens, tokens)
                                            for op in operations]
                elif 'diff' not in revision_doc:
                    if diff_pending:
                        operations = detector.diff(last_tokens, tokens)
                        revision_doc['diff'] = [op2doc(op, last_tokens, tokens)
                                                for op in operations]
                        diff_pending = False
                    else: diff_pending = True

            elif not (ismapper and first_ever_revision_doc):
                operations = detector.diff(last_tokens, tokens)
                revision_doc['diff'] = [op2doc(op, last_tokens, tokens)
                                        for op in operations]
            
            yield revision_doc
            
            last_tokens = tokens
            first_ever_revision_doc = False
            first_doc_in_group = False
            last_tokens = tokens
        
        
        if verbose: sys.stderr.write("\n")

    if ismapper and not last_revision_doc is None:
        del last_revision_doc['diff']
        last_revision_doc['text'] = last_revision_text
        yield last_revision_doc

if __name__ == "__main__": main()
