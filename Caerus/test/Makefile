# Compiler
CXX = g++

# Compiler flags
CXXFLAGS = -Wall -g `pkg-config --cflags protobuf`

# Linker flags
LDFLAGS = `pkg-config --libs protobuf`

# Proto files
PROTO_SRC = ../proto/request.pb.cc
PROTO_HDR = ../proto/request.pb.h
PROTO_OBJ = $(BUILDDIR)/request.pb.o

# Build directory
BUILDDIR = build

# All .cpp files in the current directory
SRC = $(wildcard *.cpp)

# Corresponding .o files in the build directory
OBJ = $(patsubst %.cpp,$(BUILDDIR)/%.o,$(SRC))

# Output executable in the build directory
TARGET = $(BUILDDIR)/main

# Default target
all: $(TARGET)

# Link the final executable
$(TARGET): $(OBJ) $(PROTO_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# Compile C++ source files
$(BUILDDIR)/%.o: %.cpp
	@mkdir -p $(BUILDDIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Compile protobuf source file
$(BUILDDIR)/request.pb.o: $(PROTO_SRC) $(PROTO_HDR)
	@mkdir -p $(BUILDDIR)
	$(CXX) $(CXXFLAGS) -c $(PROTO_SRC) -o $@

# Clean up
clean:
	rm -rf $(BUILDDIR)

.PHONY: all clean
