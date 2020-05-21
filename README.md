# OPUS Parser

Fetch [OPUS](http://opus.nlpl.eu/) corpora for various language pairs. The library relies on [xmltodict](https://pypi.org/project/xmltodict/) in order to use the [streaming mode](https://github.com/martinblech/xmltodict#streaming-mode) for large TMX files.

## Quickstart

Build a Docker image after installing [Docker](https://www.docker.com/products/docker-desktop) for your platform.
```
docker build -t opus_tmx_parser .
```

Simply run the prebuilt Docker image with the options for source (`-s`) and target (`-t`) language:
```
docker run -v ~/data_host:/data/ opus_tmx_parser -s en -t lv
```
You can set the data directory for the TMX files with the (`-v/--volume`) option.

## Setup
Install required packages
```
rake setup:install
```

## Run
The standard corpus (controlled with `-c` option) is ParaCrawl. To get a list of available corpora check: http://opus.nlpl.eu/opusapi/?corpora=True
```
./opus_tmx_parser.py -s en -t lt
```

## Development

### linter
Test code quality
```
rake dev:lint
```

### clean environment
Remove old cache files
```
rake dev:clean
```

## Notes

A sample output of running the script with docker `docker run -v ~/data_host:/data/ opus_tmx_parser -s en -t lv`. 
```
2020-05-21 18:27:40,577 INFO: (opus_tmx_parser.py:260) No TMX file /data/en-lv.tmx.gz for input languages en-lv found.
2020-05-21 18:27:41,190 INFO: (opus_tmx_parser.py:108) Corpus information [{'alignment_pairs': 1019003, 'corpus': 'ParaCrawl', 'documents': 21, 'id': 264034, 'latest': 'True', 'preprocessing': 'tmx', 'size': 292192, 'source': 'en', 'source_tokens': 26868814, 'target': 'lv', 'target_tokens': 24032548, 'url': 'https://object.pouta.csc.fi/OPUS-ParaCrawl/v5/tmx/en-lv.tmx.gz', 'version': 'v5'}]
2020-05-21 18:27:41,190 INFO: (opus_tmx_parser.py:117) Fetching TMX file from https://object.pouta.csc.fi/OPUS-ParaCrawl/v5/tmx/en-lv.tmx.gz
100%|██████████| 299M/299M [00:33<00:00, 8.87MiB/s]
2020-05-21 18:28:15,314 INFO: (opus_tmx_parser.py:299) Deleting pre-exisiting files /data/ParaCrawl_en.txt and /data/ParaCrawl_lv.txt
2020-05-21 18:28:15,325 INFO: (opus_tmx_parser.py:309) Processing /data/en-lv.tmx.gz writing every 300,000 lines to /data/ParaCrawl_lv.txt and /data/ParaCrawl_en.txt
2020-05-21 18:29:16,538 INFO: (opus_tmx_parser.py:230) Writing 300,000 lines of language pairs.
2020-05-21 18:30:27,960 INFO: (opus_tmx_parser.py:230) Writing 300,000 lines of language pairs.
2020-05-21 18:31:33,603 INFO: (opus_tmx_parser.py:230) Writing 300,000 lines of language pairs.
2020-05-21 18:32:12,146 INFO: (opus_tmx_parser.py:230) Writing 119,003 lines of language pairs.
```
Explanation:
* The script has 2 mandatory arguments for source and target language. OPUS releases several corpora by default I use ParaCrawl but with another option
* The script uses the OPUS API to verift that the corpus exists and that the corpus offers the specified languages. If not the script will log an error.
* If the (sometimes very large) TMX data is not already downloaded it will be fetched with [python-requests](https://requests.readthedocs.io/en/master/). The progress of the download is logged with [tqdm](https://github.com/tqdm/tqdm).
* Since the data can be very large it is best not to read to whole file into memory but except use an xml library with streaming functionality like [xmltodict](https://github.com/martinblech/xmltodict) that processes the file iteratively.
* The output can also be written iteratively to keep the memory footprint small and appending to the output files line by line. Therefore you see the script occassionally logging how many lines have been written.

Additional remarks:
* There is an [issue in xmltodict](https://github.com/martinblech/xmltodict/issues/88) that talks about making the streaming mode more pythonic with a generator. It would have been a nicer approach than using a callback function because it would avoid the global variable `_LANGUAGE_PAIRS` that was used.
* There is the option `--line_write_len` to control how often lines should be written. It can be decreased if little memory is available or increased if a lot of memory is available.
* Since data is appended to the output files in order to process the TMX files iteratively, it is necessary to delete former files. Otherwise if the script terminates there are duplicate lines when re-running with the same parameters. The option `--keep_former_output_files` controls this behaviour.
* For a combination like Afrikaans and Greek in OpenSubtitles the number of alignment_pairs is 31774 http://opus.nlpl.eu/opusapi/?corpus=OpenSubtitles&source=el&target=af&preprocessing=tmx&version=latest however looking at the file directly there are 116 017 lines with tu elements. Therefore the number of pairs is 116 017 / 2 / 2 = 29 004 (dividing by number of tu start and end lines, and tuv element lines). That is also the output when calling the script `./opus_tmx_parser.py -s af -t el -c OpenSubtitles`. Thus, I didn't include this number to include a progress bar since it's not reliable. Because the solution is iterative there is no certainty how many lines there are and when the script approximately finishes.
* There is an available library for fetching OPUS data: https://github.com/Helsinki-NLP/OpusTools, however it's designed as command line tools and this exercise was also to showcase how to use the API.
* I would have liked to further explorer memory consumption and execution speed between xmltodict and lxml (https://lxml.de/api/lxml.etree.iterparse-class.html).
