# Compiler
CXX = g++
CXXFLAGS = -Wall -g `pkg-config --cflags protobuf`
LDFLAGS = `pkg-config --libs protobuf`

# Build directory
BUILDDIR = build

# Proto files
PROTO_SRC = ../proto/request.pb.cc
PROTO_HDR = ../proto/request.pb.h
PROTO_OBJ = $(BUILDDIR)/request.pb.o

# Ensure build dir exists
$(BUILDDIR):
	@mkdir -p $(BUILDDIR)

# Protobuf object
$(BUILDDIR)/request.pb.o: $(PROTO_SRC) $(PROTO_HDR) | $(BUILDDIR)
	$(CXX) $(CXXFLAGS) -c $(PROTO_SRC) -o $@

# -----------------------------
# Test targets
# -----------------------------
# "make batcher" -> build/batcher_test
batcher: $(BUILDDIR)/batcher_test

# "make ps" -> build/partial_sequencer_test
ps: $(BUILDDIR)/partial_sequencer_test

# Generic rule: "make foo_test" compiles foo_test.cpp into build/foo_test
%_test: $(BUILDDIR)/%_test
$(BUILDDIR)/%_test: %_test.cpp $(PROTO_OBJ) | $(BUILDDIR)
	$(CXX) $(CXXFLAGS) -o $@ $< $(PROTO_OBJ) $(LDFLAGS)

# -----------------------------
# Cleanup
# -----------------------------
clean:
	rm -rf $(BUILDDIR)

.PHONY: clean batcher ps
