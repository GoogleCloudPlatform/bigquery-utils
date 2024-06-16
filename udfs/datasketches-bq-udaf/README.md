# Requirements

- emscripten
- make
- gcloud ( optional, only required if using run.sh )

# How to build

```
git submodule init 
git submodule update
make all
```

Depending on your version of emscripten / llvm, this may raise several warnings;
they should be harmless.

For example, if you are building kll sketches, This will generate three files:

- `kll_sketch.mjs` - the library for UDAFs (uses ES6 export)
- `kll_sketch.js` - the library for UDFs (uses inline definition)
- `kll_sketch.wasm` - not used except for experimentation

# How to install

Upload `kll_sketch.mjs` and `kll_sketch.js` to a GCP bucket under
your control.

Replace all references to the `$BUCKET` with the name of the GCP
bucket where you uploaded the libraries. Install the UD(A)F functions
by executing the SQL queries in `kll_sketch.sql`.

OR simply execute `install.sh` which 
1) compiles cpp to webassembly 
2) Uploads compiled files to gcs 
3) Creates UD(A)Fs in bigquery dataset

Usage:  
`gcloud auth login`

```
./install.sh \ 
    -p|--project-id <bq-project-id> \ 
    -d|--dataset <bq-dataset> \
    -g|--gcs-path <gs://bucket/[path-to-folder]>' \
    -s|--sketches {theta,kll,tuple}
```

## Example 

Install theta & tuple sketch functions:   
```
./install.sh -p <project> -d <dataset> -g gs://<bucket> -s theta,tuple
```
