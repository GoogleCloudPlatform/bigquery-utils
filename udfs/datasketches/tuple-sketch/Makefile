OUT_DIR=../../js_builds
EMCC=emcc
EMCFLAGS=-I../datasketches-cpp/tuple/include \
	-I../datasketches-cpp/common/include \
	-I../datasketches-cpp/theta/include \
	--no-entry \
	-sWASM_BIGINT=1 \
	-sEXPORTED_FUNCTIONS=[_malloc,_free] \
	-sENVIRONMENT=shell \
	-sTOTAL_MEMORY=1024MB \
	-sNO_DISABLE_EXCEPTION_CATCHING \
	-o $(OUT_DIR)/$@ \
	-O3

$(shell mkdir -p $(OUT_DIR))

all: tuple_sketch.mjs tuple_sketch.js tuple_sketch.wasm

tuple_sketch.mjs: tuple_sketch.cpp
	$(EMCC) $< $(EMCFLAGS) -sSINGLE_FILE=1

# this rule creates a non-es6 loadable library
tuple_sketch.js: tuple_sketch.cpp
	$(EMCC) $< $(EMCFLAGS) -sSINGLE_FILE=1

tuple_sketch.wasm: tuple_sketch.cpp
	$(EMCC) $< $(EMCFLAGS) -sSTANDALONE_WASM=1


clean:
	$(RM) $(OUT_DIR)/tuple_sketch.mjs $(OUT_DIR)/tuple_sketch.js $(OUT_DIR)/tuple_sketch.wasm

.PHONY: clean
