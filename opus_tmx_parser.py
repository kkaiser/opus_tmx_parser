#!/usr/bin/env python3


"""Fetch OPUS (http://opus.nlpl.eu/) corpora for various language pairs."""

import gzip
import logging
import os
from argparse import ArgumentParser
from functools import partial

import requests
import xmltodict
from tqdm import tqdm

logging.basicConfig(
    format='%(asctime)s %(levelname)s: (%(filename)s:%(lineno)d) %(message)s',
    level=logging.INFO,
)
_LOGGER = logging.getLogger()

_OPUS_API_URL = 'http://opus.nlpl.eu/opusapi/'
_DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')
_LANGUAGE_PAIRS = []


def _get_args():
    parser = ArgumentParser()

    parser.add_argument(
        '-s',
        '--source_language_code',
        help=(
            'ISO language code of source language '
            'that will be translated into target language',
        ),
        required=True,
    )

    parser.add_argument(
        '-t',
        '--target_language_code',
        help='ISO language code of target language to translate to',
        required=True,
    )

    parser.add_argument(
        '-c',
        '--corpus',
        help='Parallel corpus type to select',
        default='ParaCrawl',
    )

    parser.add_argument(
        '-k',
        '--keep_former_output_files',
        help=(
            'Delete previously generated line separated language output files'
        ),
        default=False,
        action='store_true',
    )

    parser.add_argument(
        '-l',
        '--line_write_len',
        help=(
            'Length of lines that are saved in memory until '
            'they are written to output files'
        ),
        default=300000,
        type=int,
    )

    return parser.parse_args()


def _clean_line(text):
    return text.replace('\n', ' ').strip()


def _is_bad_response(rsp):
    if not rsp.ok:
        _LOGGER.error(
            f"Couldn't fetch URL {rsp.url}. OPUS API {_OPUS_API_URL} offline?",
        )
        return True

    return False


def _fetch_and_write_tmx_file(corpus, source_lang, target_lang, tmx_fname):
    payload = {
        'corpus': corpus,
        'source': source_lang,
        'target': target_lang,
        'preprocessing': 'tmx',
        'version': 'latest',
    }

    rsp = requests.get(_OPUS_API_URL, params=payload)
    if _is_bad_response(rsp):
        return False

    json_rsp = rsp.json()
    corpus = json_rsp.get('corpora', [])
    if not corpus:
        _LOGGER.error(f'Bad response on {rsp.url} - unknown JSON {json_rsp}')
        return False

    _LOGGER.info(f'Corpus information {corpus}')

    # there is only one TMX version
    # alternatives only exist for XML
    tmx_url = corpus[0].get('url')
    if not tmx_url:
        _LOGGER.error(f'Bad response on {rsp.url} - no TMX url {corpus}')
        return False

    _LOGGER.info(f'Fetching TMX file from {tmx_url}')
    tmx_rsp = requests.get(tmx_url, stream=True)
    if _is_bad_response(tmx_rsp):
        return False

    total_size = int(tmx_rsp.headers.get('content-length', 0))
    block_size = 1024
    progr = tqdm(total=total_size, unit='iB', unit_scale=True)

    with open(tmx_fname, 'wb') as fp:
        for data in tmx_rsp.iter_content(block_size):
            progr.update(len(data))
            fp.write(data)

    progr.close()
    if total_size != 0 and progr.n != total_size:
        _LOGGER.error(
            f"Couldn't write full TMX file {tmx_fname} from URL {tmx_rsp.url}",
        )
        return False

    return True


def _requested_corpus_exists(corpus):
    payload = {
        'corpora': 'True',
    }
    rsp = requests.get(_OPUS_API_URL, params=payload)
    if _is_bad_response(rsp):
        return False

    opus_corpora = rsp.json().get('corpora', [])
    if corpus not in opus_corpora:
        _LOGGER.error(f'Bad corpus "{corpus}" must be one in {opus_corpora}')
        return False

    return True


def _requested_languages_exist(corpus, source_lang, target_lang):
    payload_source_check = {
        'languages': 'True',
        'corpus': corpus,
    }
    rsp = requests.get(_OPUS_API_URL, params=payload_source_check)
    if _is_bad_response(rsp):
        return False

    source_languages = rsp.json().get('languages', [])
    if source_lang not in source_languages:
        _LOGGER.error(
            f'Bad source language "{source_lang}" '
            f'must be one in {source_languages} for corpus {corpus}',
        )
        return False

    payload_target_check = {
        'languages': 'True',
        'corpus': corpus,
        'source': source_lang,
    }
    rsp = requests.get(_OPUS_API_URL, params=payload_target_check)
    if _is_bad_response(rsp):
        return False

    target_languages = rsp.json().get('languages', [])
    if target_lang not in target_languages:
        _LOGGER.error(
            f'Bad target language "{target_lang}" '
            f'must be one in {target_languages} for corpus {corpus}',
        )
        return False

    return True


def _parse_language_pairs(
    source_fname,
    source_lang,
    target_fname,
    target_lang,
    line_write_len,
    path,
    tree_dict,
):
    language_pair = {}
    for elem in tree_dict['tuv']:
        language_pair[elem['@xml:lang']] = elem['seg']

    if all(lang in language_pair for lang in (source_lang, target_lang)):
        _LANGUAGE_PAIRS.append(language_pair)

    else:
        _LOGGER.debug(
            f"Ignoring tu ID {path[2][1].get('tuid')} "
            f'with missing language {source_lang} or {target_lang}',
        )

    if (
        len(_LANGUAGE_PAIRS) != 0
        and len(_LANGUAGE_PAIRS) % line_write_len == 0
    ):
        _write_language_pairs(
            source_fname, source_lang, target_fname, target_lang,
        )

    return True


def _write_language_pairs(
    source_fname, source_lang, target_fname, target_lang,
):
    _LOGGER.info(f'Writing {len(_LANGUAGE_PAIRS):,} lines of language pairs.')

    with open(source_fname, 'at') as s_fp, open(target_fname, 'at') as t_fp:
        for language_pair in _LANGUAGE_PAIRS:
            s_fp.write(_clean_line(language_pair[source_lang]) + '\n')
            t_fp.write(_clean_line(language_pair[target_lang]) + '\n')

    # clean list to save memory
    del _LANGUAGE_PAIRS[:]


def main():
    args = _get_args()

    if not _requested_corpus_exists(args.corpus):
        return

    if not _requested_languages_exist(
        args.corpus, args.source_language_code, args.target_language_code,
    ):
        return

    if not os.path.isdir(_DATA_DIR):
        os.mkdir(_DATA_DIR)

    tmx_fname = os.path.join(
        _DATA_DIR,
        f'{args.source_language_code}-{args.target_language_code}.tmx.gz',
    )
    if not os.path.isfile(tmx_fname):
        _LOGGER.info(
            f'No TMX file {tmx_fname} for input languages '
            f'{args.source_language_code}-{args.target_language_code} found.',
        )

        if not _fetch_and_write_tmx_file(
            args.corpus,
            args.source_language_code,
            args.target_language_code,
            tmx_fname,
        ):
            _LOGGER.error(
                f'TMX file could not be fetched and saved as {tmx_fname}',
            )
            return

    source_fname = os.path.join(
        _DATA_DIR,
        f'{args.corpus}_{args.source_language_code}.txt',
    )
    target_fname = os.path.join(
        _DATA_DIR,
        f'{args.corpus}_{args.target_language_code}.txt',
    )
    parse_language_pairs = partial(
        _parse_language_pairs,
        source_fname,
        args.source_language_code,
        target_fname,
        args.target_language_code,
        args.line_write_len,
    )

    if args.keep_former_output_files:
        _LOGGER.info(
            f'Appending to exisiting files {target_fname} and {source_fname}',
        )

    else:
        _LOGGER.info(
            f'Deleting pre-exisiting files {source_fname} and {target_fname}',
        )

        if os.path.isfile(source_fname):
            os.remove(source_fname)

        if os.path.isfile(target_fname):
            os.remove(target_fname)

    _LOGGER.info(
        f'Processing {tmx_fname} writing every {args.line_write_len:,} '
        f'lines to {target_fname} and {source_fname}',
    )

    with gzip.open(tmx_fname, 'r') as fp:
        # item depth for structure: <tmx> <body> <tu>
        xmltodict.parse(
            fp, item_depth=3, item_callback=parse_language_pairs,
        )

    # write remaining language pairs < line_write_len
    _write_language_pairs(
        source_fname,
        args.source_language_code,
        target_fname,
        args.target_language_code,
    )


if __name__ == '__main__':
    main()
