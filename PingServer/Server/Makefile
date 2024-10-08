# Compiler
CXX = g++

# Compiler flags
CXXFLAGS = -Wall -g

# All .cpp files in the current directory
SRC = $(wildcard *.cpp)

# Corresponding .o files
OBJ = $(SRC:.cpp=.o)

# Output executable
TARGET = server

# Rule to build the target
all: $(TARGET)

$(TARGET): $(OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^

# Rule to build .o files from .cpp files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean up object files and executable
clean:
	rm -f $(OBJ) $(TARGET)

# PHONY to prevent conflicts with files named "clean"
.PHONY: clean