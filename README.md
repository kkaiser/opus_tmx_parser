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
* Since data is appended to the output files in order to process the TMX files iteratively, it is necessary to delete former files. Otherwise if the script terminates there are duplicate lines when re-running with the same parameters. The option `--keep_former_output_files` controls this behaviour.
* For a combination like Afrikaans and Greek in OpenSubtitles the number of alignment_pairs is 31774 http://opus.nlpl.eu/opusapi/?corpus=OpenSubtitles&source=el&target=af&preprocessing=tmx&version=latest however looking at the file directly there are 116 017 lines with tu elements. Therefore the number of pairs is 116 017 / 2 / 2 = 29 004 (dividing by number of tu start and end lines, and tuv element lines). That is also the output when calling the script `./opus_tmx_parser.py -s af -t el -c OpenSubtitles`. Thus, I didn't include this number to include a progress bar since it's not reliable. Because the solution is iterative there is no certainty how many lines there are and when the script approximately finishes.
* There is an available library for fetching OPUS data: https://github.com/Helsinki-NLP/OpusTools, however it's designed as command line tools and this exercise was also to showcase how to use the API.
* I would have liked to further explorer memory consumption and execution speed between xmltodict and lxml (https://lxml.de/api/lxml.etree.iterparse-class.html).
