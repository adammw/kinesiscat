# kinesiscat [![Build Status](https://travis-ci.com/adammw/kinesiscat.png)](https://travis-ci.com/adammw/kinesiscat) [![Coverage](https://img.shields.io/badge/Coverage-100%25-green.svg)](https://github.com/grosser/go-testcov)


Netcat for [AWS Kinesis Data Streams](https://aws.amazon.com/kinesis/data-streams/).

Inspired by [edenhill/kafkacat](https://github.com/edenhill/kafkacat).


## Usage

```bash
go get https://github.com/adammw/kinesiscat
kinesiscat [OPTIONS] <STREAM_NAME>
```

Requirements:
* `<STREAM_NAME>` argument
* `--region <REGION>` or set `$AWS_REGION`
* `~/.aws/credentials` or set `$AWS_ACCESS_KEY_ID` and `$AWS_SECRET_KEY`


## Alternatives

* https://github.com/volker48/c2k (GET and PUT)
* https://github.com/robbles/kinesiscat (GET only)
* https://github.com/winebarrel/kinesis-cat-go (PUT only)
* https://github.com/winebarrel/kinesis_cat (PUT only)


## Author

[Adam Malcontenti-Wilson](https://github.com/adammw)
adman.com@gmail.com
License: MIT
