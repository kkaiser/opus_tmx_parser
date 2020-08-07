#!/usr/bin/env python3


"""Fetch OPUS (http://opus.nlpl.eu/) corpora for various language pairs."""

import gzip
import logging
import os
import unicodedata
from argparse import ArgumentParser
from functools import lru_cache, partial

import cld3
import requests
import xmltodict
from tqdm import tqdm

logging.basicConfig(
    format='%(asctime)s %(levelname)s: (%(filename)s:%(lineno)d) %(message)s',
    level=logging.INFO,
)
LOGGER = logging.getLogger()

DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')

# https://github.com/google/cld3/blob/master/src/task_context_params.cc#L43
CLD3_LANG_CODES = {
    'eo', 'co', 'eu', 'ta', 'de', 'mt', 'ps', 'te', 'su', 'uz', 'zh-Latn',
    'ne', 'nl', 'sw', 'sq', 'hmn', 'ja', 'no', 'mn', 'so', 'ko', 'kk', 'sl',
    'ig', 'mr', 'th', 'zu', 'ml', 'hr', 'bs', 'lo', 'sd', 'cy', 'hy', 'uk',
    'pt', 'lv', 'iw', 'cs', 'vi', 'jv', 'be', 'km', 'mk', 'tr', 'fy', 'am',
    'zh', 'da', 'sv', 'fi', 'ht', 'af', 'la', 'id', 'fil', 'sm', 'ca', 'el',
    'ka', 'sr', 'it', 'sk', 'ru', 'ru-Latn', 'bg', 'ny', 'fa', 'haw', 'gl',
    'et', 'ms', 'gd', 'bg-Latn', 'ha', 'is', 'ur', 'mi', 'hi', 'bn', 'hi-Latn',
    'fr', 'yi', 'hu', 'xh', 'my', 'tg', 'ro', 'ar', 'lb', 'el-Latn', 'st',
    'ceb', 'kn', 'az', 'si', 'ky', 'mg', 'en', 'gu', 'es', 'pl', 'ja-Latn',
    'ga', 'lt', 'sn', 'yo', 'pa', 'ku',
}
OPUS_API_URL = 'http://opus.nlpl.eu/opusapi/'

LANGUAGE_PAIRS = []
# this is necessary because of bad design of xmltodict
# see https://github.com/martinblech/xmltodict/issues/88
LINES_LEFT = 0
MAX_NONALPHA_RATIO = .1


def _alphabet(text):
    for ch in text:
        if unicodedata.category(ch) in ['Lu', 'Ll', 'Lo']:
            char_name = unicodedata.name(ch, None)
            if char_name is not None:
                return char_name.split()[0]


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

    rsp = requests.get(OPUS_API_URL, params=payload)
    if _is_bad_response(rsp):
        return False

    json_rsp = rsp.json()
    corpus = json_rsp.get('corpora', [])
    if not corpus:
        LOGGER.error(f'Bad response on {rsp.url} - unknown JSON {json_rsp}')
        return False

    LOGGER.info(f'Corpus information {corpus}')

    # there is only one TMX version
    # alternatives only exist for XML
    tmx_url = corpus[0].get('url')
    if not tmx_url:
        LOGGER.error(f'Bad response on {rsp.url} - no TMX url {corpus[0]}')
        return False

    LOGGER.info(f'Fetching TMX file from {tmx_url}')
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
        LOGGER.error(
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
    lang_data_dir,
    line_write_len,
):
    if not _requested_corpus_exists(corpus):
        return

    if not _requested_languages_exist(corpus, source_lang, target_lang):
        return

    tmx_fname = os.path.join(
        lang_data_dir, f'{corpus}-{source_lang}_{target_lang}.tmx.gz',
    )
    if not os.path.isfile(tmx_fname):
        LOGGER.info(
            f'No TMX file {tmx_fname} for input languages '
            f'{source_lang}-{target_lang} found.',
        )

        if not _fetch_and_write_tmx_file(
            corpus, source_lang, target_lang, tmx_fname,
        ):
            LOGGER.error(
                f'TMX file could not be fetched and saved as {tmx_fname}',
            )
            return

    parse_language_pairs = partial(
        _parse_language_pairs,
        source_fname, source_lang, target_fname, target_lang, line_write_len,
    )

    LOGGER.info(
        f'Processing {corpus} from {tmx_fname}\n'
        f'writing {LINES_LEFT:,} lines in {line_write_len:,} chunks\n'
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

    rsp = requests.get(OPUS_API_URL, params=payload)
    if _is_bad_response(rsp):
        return False

    json_rsp = rsp.json()
    corpora = json_rsp.get('corpora', [])
    if not corpora:
        LOGGER.error(f'Bad response on {rsp.url} - unknown JSON {json_rsp}')
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
        LOGGER.error(f"rsp.status couldn't fetch URL {rsp.url}")
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
    global LINES_LEFT
    if LINES_LEFT <= 0:
        return

    language_pair = {}
    for elem in tree_dict['tuv']:
        language_pair[elem['@xml:lang']] = elem['seg']

    if all(
        _validate_sentence(language_pair.get(lang, ''), lang)
        for lang in (source_lang, target_lang)
    ):
        LANGUAGE_PAIRS.append(language_pair)

    else:
        try:
            LOGGER.debug(
                f"Ignoring tu ID {path[2][1].get('tuid')} "
                f'with missing language {source_lang} or {target_lang}',
            )
        except (IndexError, AttributeError):
            pass

    if (
        len(LANGUAGE_PAIRS) != 0
        and (
            len(LANGUAGE_PAIRS) % line_write_len == 0
            or len(LANGUAGE_PAIRS) >= LINES_LEFT
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
    rsp = requests.get(OPUS_API_URL, params=payload)
    if _is_bad_response(rsp):
        return False

    opus_corpora = rsp.json().get('corpora', [])
    if corpus not in opus_corpora:
        LOGGER.error(f'Bad corpus "{corpus}" must be one in {opus_corpora}')
        return False

    return True


def _requested_languages_exist(corpus, source_lang, target_lang):
    payload_source_check = {
        'languages': 'True',
        'corpus': corpus,
    }
    rsp = requests.get(OPUS_API_URL, params=payload_source_check)
    if _is_bad_response(rsp):
        return False

    source_languages = rsp.json().get('languages', [])
    if source_lang not in source_languages:
        LOGGER.error(
            f'Bad source language "{source_lang}" '
            f'must be one in {source_languages} for corpus {corpus}',
        )
        return False

    payload_target_check = {
        'languages': 'True',
        'corpus': corpus,
        'source': source_lang,
    }
    rsp = requests.get(OPUS_API_URL, params=payload_target_check)
    if _is_bad_response(rsp):
        return False

    target_languages = rsp.json().get('languages', [])
    if target_lang not in target_languages:
        LOGGER.error(
            f'Bad target language "{target_lang}" '
            f'must be one in {target_languages} for corpus {corpus}',
        )
        return False

    return True


@lru_cache(maxsize=128)
def _validate_language(text, language):
    if language not in CLD3_LANG_CODES:
        return True

    lang_res = cld3.get_language(text)
    if lang_res.is_reliable and lang_res.language == language:
        return True

    return False


def _validate_sentence(text, language):
    if (
        _alphabet(text) != 'LATIN'
        or not _validate_language(text, language)
    ):
        return False

    nonalpha_count = sum(
        1 for c in text if not (c.isalpha() or c.isspace())
    )

    return nonalpha_count / len(text) < MAX_NONALPHA_RATIO


def _write_language_pairs(
    source_fname, source_lang, target_fname, target_lang,
):
    global LINES_LEFT
    if LINES_LEFT <= 0:
        return

    LOGGER.info(
        f'Writing {len(LANGUAGE_PAIRS):,} lines of language pairs '
        f'{source_lang}-{target_lang} lines left to write: {LINES_LEFT}',
    )

    with open(source_fname, 'at') as s_fp, open(target_fname, 'at') as t_fp:
        for language_pair in LANGUAGE_PAIRS:
            if LINES_LEFT <= 0:
                break

            s_fp.write(_clean_line(language_pair[source_lang]) + '\n')
            t_fp.write(_clean_line(language_pair[target_lang]) + '\n')
            LINES_LEFT -= 1

    # clean list to save memory
    del LANGUAGE_PAIRS[:]


def main():
    args = _get_args()

    global LINES_LEFT
    LINES_LEFT = args.max_lines

    for lang_type, lang_code in {
        'source': args.source_language_code,
        'target': args.target_language_code,
    }.items():
        if lang_code not in CLD3_LANG_CODES:
            LOGGER.warning(
                f'TMX files for {lang_type} language {lang_code} '
                "can't be validated with cld2 language detector",
            )

    lang_data_dir = os.path.join(
        DATA_DIR, f'{args.source_language_code}_{args.target_language_code}',
    )
    if not os.path.isdir(lang_data_dir):
        os.makedirs(lang_data_dir)

    source_fname = os.path.join(
        lang_data_dir, f'{args.source_language_code}.txt',
    )
    target_fname = os.path.join(
        lang_data_dir, f'{args.target_language_code}.txt',
    )
    if args.keep_former_output_files:
        LOGGER.info(
            f'Appending to exisiting files {target_fname} and {source_fname}',
        )

    else:
        LOGGER.info(
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
            LOGGER.error(
                f'No corpora for language pair {args.source_language_code}-'
                f'{args.target_language_code}',
            )
            return

        lines_per_corpus = int(LINES_LEFT / len(corpora))
        for corpus in corpora:
            if lines_per_corpus > 0:
                LINES_LEFT = lines_per_corpus
            else:
                LINES_LEFT = args.max_lines

            _fetch_corpus(
                corpus,
                source_fname,
                target_fname,
                args.source_language_code,
                args.target_language_code,
                lang_data_dir,
                args.line_write_len,
            )

    else:
        _fetch_corpus(
            corpus,
            source_fname,
            target_fname,
            args.source_language_code,
            args.target_language_code,
            lang_data_dir,
            args.line_write_len,
        )


if __name__ == '__main__':
    main()
