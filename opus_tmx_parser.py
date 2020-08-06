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
_LINES_LEFT = 0

def _clean_line(text):
    return text.replace('\n', ' ').strip()


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
        _LOGGER.error(f'Bad response on {rsp.url} - no TMX url {corpus[0]}')
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


def _fetch_corpus(
    corpus,
    source_fname,
    target_fname,
    source_lang,
    target_lang,
    line_write_len,
):
    if not _requested_corpus_exists(corpus):
        return

    if not _requested_languages_exist(corpus, source_lang, target_lang):
        return

    if not os.path.isdir(_DATA_DIR):
        os.mkdir(_DATA_DIR)

    tmx_fname = os.path.join(
        _DATA_DIR, f'{corpus}-{source_lang}_{target_lang}.tmx.gz',
    )
    if not os.path.isfile(tmx_fname):
        _LOGGER.info(
            f'No TMX file {tmx_fname} for input languages '
            f'{source_lang}-{target_lang} found.',
        )

        if not _fetch_and_write_tmx_file(
            corpus, source_lang, target_lang, tmx_fname,
        ):
            _LOGGER.error(
                f'TMX file could not be fetched and saved as {tmx_fname}',
            )
            return

    parse_language_pairs = partial(
        _parse_language_pairs,
        source_fname, source_lang, target_fname, target_lang, line_write_len,
    )

    _LOGGER.info(
        f'Processing {corpus} from {tmx_fname}\n'
        f'writing {_LINES_LEFT:,} lines in {line_write_len:,} chunks\n'
        f'to {target_fname} and {source_fname}',
    )

    with gzip.open(tmx_fname, 'r') as fp:
        try:
            # item depth for structure: <tmx> <body> <tu>
            xmltodict.parse(
                fp, item_depth=3, item_callback=parse_language_pairs,
            )
        except xmltodict.ParsingInterrupted:
            pass

    _write_language_pairs(source_fname, source_lang, target_fname, target_lang)


def _fetch_corpora(source_lang, target_lang):
    payload = {
        'source': source_lang,
        'target': target_lang,
        'preprocessing': 'tmx',
        'version': 'latest',
    }

    rsp = requests.get(_OPUS_API_URL, params=payload)
    if _is_bad_response(rsp):
        return False

    json_rsp = rsp.json()
    corpora = json_rsp.get('corpora', [])
    if not corpora:
        _LOGGER.error(f'Bad response on {rsp.url} - unknown JSON {json_rsp}')
        return False

    return [
        corpus_info['corpus'] for corpus_info in corpora
        if corpus_info.get('corpus')
    ]


def _get_args():
    parser = ArgumentParser()

    parser.add_argument(
        '-s',
        '--source_language_code',
        help=(
            'ISO language code of source language '
            'that will be translated into target language'
        ),
        required=True,
    )

    parser.add_argument(
        '-t',
        '--target_language_code',
        help='ISO language code of target language to translate to',
        default='en',
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
        default=300000,  # 300k
        type=int,
    )

    parser.add_argument(
        '-m',
        '--max_lines',
        help='Maximum length of lines that are saved per language pair',
        default=100000000,  # 100M
        type=int,
    )

    crawl_group = parser.add_mutually_exclusive_group()

    crawl_group.add_argument(
        '-a',
        '--all_corpora',
        help='Parallel corpus type to select',
        default=False,
        action='store_true',
    )

    crawl_group.add_argument(
        '-c',
        '--corpus',
        help='Parallel corpus type to select',
        default='ParaCrawl',
    )

    return parser.parse_args()


def _is_bad_response(rsp):
    if not rsp.ok:
        _LOGGER.error(f"rsp.status couldn't fetch URL {rsp.url}")
        return True

    return False


def _parse_language_pairs(
    source_fname,
    source_lang,
    target_fname,
    target_lang,
    line_write_len,
    path,
    tree_dict,
):
    global _LINES_LEFT
    if _LINES_LEFT <= 0:
        return

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
        and (
            len(_LANGUAGE_PAIRS) % line_write_len == 0
            or len(_LANGUAGE_PAIRS) >= _LINES_LEFT
        )
    ):
        _write_language_pairs(
            source_fname, source_lang, target_fname, target_lang,
        )

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


def _write_language_pairs(
    source_fname, source_lang, target_fname, target_lang,
):
    global _LINES_LEFT
    if _LINES_LEFT <= 0:
        return

    _LOGGER.info(
        f'Writing {len(_LANGUAGE_PAIRS):,} lines of language pairs '
        f'{source_lang}-{target_lang} lines left to write: {_LINES_LEFT}',
    )

    with open(source_fname, 'at') as s_fp, open(target_fname, 'at') as t_fp:
        for language_pair in _LANGUAGE_PAIRS:
            if _LINES_LEFT <= 0:
                break

            s_fp.write(_clean_line(language_pair[source_lang]) + '\n')
            t_fp.write(_clean_line(language_pair[target_lang]) + '\n')
            _LINES_LEFT -= 1

    # clean list to save memory
    del _LANGUAGE_PAIRS[:]


def main():
    args = _get_args()

    global _LINES_LEFT
    _LINES_LEFT = args.max_lines

    source_fname = os.path.join(_DATA_DIR, f'{args.source_language_code}.txt')
    target_fname = os.path.join(_DATA_DIR, f'{args.target_language_code}.txt')
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

    if args.all_corpora:

        corpora = _fetch_corpora(
            args.source_language_code, args.target_language_code,
        )
        if not corpora:
            _LOGGER.error(
                f'No corpora for language pair {args.source_language_code}-'
                f'{args.target_language_code}',
            )
            return

        lines_per_corpus = int(_LINES_LEFT / len(corpora))
        for corpus in corpora:
            if lines_per_corpus > 0:
                _LINES_LEFT = lines_per_corpus
            else:
                _LINES_LEFT = args.max_lines

            _fetch_corpus(
                corpus,
                source_fname,
                target_fname,
                args.source_language_code,
                args.target_language_code,
                args.line_write_len,
            )

    else:
        _fetch_corpus(
            corpus,
            source_fname,
            target_fname,
            args.source_language_code,
            args.target_language_code,
            args.line_write_len,
        )


if __name__ == '__main__':
    main()
