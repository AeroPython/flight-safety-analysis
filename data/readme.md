# Getting the data

As the database is too big to be uploaded to the repository it is stored separately [here](https://www.dropbox.com/s/n9inalri0dvff1j/avall.db?dl=1). It can be placed in the `data` folder manually or downloaded using the `get_data.py` python script.

The data can also be obtained directly from NTSB website (https://app.ntsb.gov/avdata/) in `mdb` format. In order to convert it to a SQLite database the script `export_access2csv.sh` in `utils` folder can be used:

```
$ bash utils/export_access2csv.sh data/raw/avall.mdb data
```

* avall.db -> NTSB aviation data from the new NTSB accident database system (eADMS)
