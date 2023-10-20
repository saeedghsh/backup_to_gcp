# Back up to Google Cloud Storage

Entry points:
```bash
python3 src/backup_to_gcp_bucket.py --help
python3 src/compare_directories.py --help
```

# Laundy list
* [ ] schedule recurring backup.
* [x] modify the script, so that the local changes (e.g. moved or renamed directories) would be mirrored on the cloud. This effectively requires removing remote files that are no longer available locally.
* [x] implement the use of argument `operation`.

# License
```
Copyright (C) Saeed Gholami Shahbandi
```

NOTE: Portions of this code/project were developed with the assistance of ChatGPT, a product of OpenAI.

Distributed with a GNU GENERAL PUBLIC LICENSE; see [LICENSE](https://github.com/saeedghsh/backup_to_gcp/blob/master/LICENSE).

