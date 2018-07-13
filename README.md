# kinesiscat

Netcat for [AWS Kinesis Data Streams](https://aws.amazon.com/kinesis/data-streams/).

Inspired by [edenhill/kafkacat](https://github.com/edenhill/kafkacat).

## Usage

`kinesiscat [OPTIONS] <STREAM_NAME>`

Requirements:
* `<STREAM_NAME>` argument
* `--region <REGION>` or set `$AWS_REGION`
* `~/.aws/credentials` or set `$AWS_ACCESS_KEY_ID` and `$AWS_SECRET_KEY`

## Alternatives
* https://github.com/volker48/c2k (GET and PUT)
* https://github.com/robbles/kinesiscat (GET only)
* https://github.com/winebarrel/kinesis-cat-go (PUT only)
* https://github.com/winebarrel/kinesis_cat (PUT only)
