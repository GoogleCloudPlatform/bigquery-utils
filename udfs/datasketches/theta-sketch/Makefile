OUT_DIR=../../js_builds
EMCC=emcc
EMCFLAGS=-I../datasketches-cpp/theta/include \
	-I../datasketches-cpp/common/include \
	--no-entry \
	-sWASM_BIGINT=1 \
	-sEXPORTED_FUNCTIONS=[_malloc,_free] \
	-sENVIRONMENT=shell \
	-sTOTAL_MEMORY=1024MB \
	--pre-js bytes_to_base64.js \
	-o $(OUT_DIR)/$@ \
	-O3

$(shell mkdir -p $(OUT_DIR))

all: theta_sketch.mjs theta_sketch.js theta_sketch.wasm

theta_sketch.mjs: theta_sketch.cpp
	$(EMCC) $< $(EMCFLAGS) -sSINGLE_FILE=1

# this rule creates a non-es6 loadable library
theta_sketch.js: theta_sketch.cpp
	$(EMCC) $< $(EMCFLAGS) -sSINGLE_FILE=1

theta_sketch.wasm: theta_sketch.cpp
	$(EMCC) $< $(EMCFLAGS) -sSTANDALONE_WASM=1


clean:
	$(RM) $(OUT_DIR)/theta_sketch.mjs $(OUT_DIR)/theta_sketch.js $(OUT_DIR)/theta_sketch.wasm

.PHONY: clean
