OUT_DIR=../../js_builds
EMCC=emcc
EMCFLAGS=-I../datasketches-cpp/kll/include \
	-I../datasketches-cpp/common/include \
	--no-entry \
	-sWASM_BIGINT=1 \
	-sEXPORTED_FUNCTIONS=[_malloc,_free] \
	-sENVIRONMENT=shell \
	-sTOTAL_MEMORY=1024MB \
	--pre-js crypto.js \
	-o $(OUT_DIR)/$@ \
	-O3

$(shell mkdir -p $(OUT_DIR))

all: kll_sketch.mjs kll_sketch.js kll_sketch.wasm

kll_sketch.mjs: kll_sketch.cpp
	$(EMCC) $< $(EMCFLAGS) -sSINGLE_FILE=1

# this rule creates a non-es6 loadable library
kll_sketch.js: kll_sketch.cpp
	$(EMCC) $< $(EMCFLAGS) -sSINGLE_FILE=1

kll_sketch.wasm: kll_sketch.cpp
	$(EMCC) $< $(EMCFLAGS) -sSTANDALONE_WASM=1


clean:
	$(RM) $(OUT_DIR)/kll_sketch.mjs $(OUT_DIR)/kll_sketch.js $(OUT_DIR)/kll_sketch.wasm

.PHONY: clean