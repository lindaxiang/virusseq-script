# Scripts to download and update metadata analysis in batch for Virusseq portal

## Download song analysis in batch from song server
```
./get-song-dump.py -s study_id -m song_url -d file_dump
```
### Inputs
- study_id: Provide `study_id` you want to retrieve all the analysis from the server. If not provide, will download for all studies.
- song_url: Provide `song_url` of the song server. Default: `https://song.dev.cancogen.cancercollaboratory.org`.
- file_dump: Provide the path prefix to the file_dump. Default: `data/virusseq-song`.

### Outputs
You will find song-dump file under folder `data/`. E.g,
```
data
└── virusseq-song.UHTC-ON.2022-06-23.jsonl
```



## Prepare the metadata update dump
```
./prep-migration.py -d dump_path -s source_metadata_files -e env
```
### Inputs
- dump_path: Path to song dump jsonl file.
- source_metadata_files: Source metadata files to get information for requested fields.
- env: Specify environment to prepare the migration payloads. Default: `dev`

### Outputs
- You will find payloads for updating being generated under folder `virusseq_song_update/dev/date`. E.g,
```
virusseq_song_update/dev
└── 2022-06-27
    ├── migrate_consensus_sequence.UHTC-ON.jsonl
```
- You can print the running logs into a log file for verification.



## Execute the song payload updating
```
./song-migrate.py -d dump_path -s song_url -t token -x exclude
```

### Inputs
- dump_path: Path to migrated dump jsonl file. Required
- song_url: Song server URL. Default: `https://song.dev.cancogen.cancercollaboratory.org`
- token: Access token. Required
- exclude: The list of analysis to exclude from updating. Optional

### Outputs
- The song analysis will be updated, and portal will pick up the updated changes and reflect them on the portal. 
- You can print the running logs into a log file for verification.

