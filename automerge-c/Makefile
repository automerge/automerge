
CC=gcc
CFLAGS=-I.
DEPS=automerge.h
LIBS=-lpthread -ldl -lm
LDIR=../target/release
LIB=../target/release/libautomerge.a
DEBUG_LIB=../target/debug/libautomerge.a

all: $(DEBUG_LIB) automerge

debug: LDIR=../target/debug
debug: automerge $(DEBUG_LIB)

automerge: automerge.o $(LDIR)/libautomerge.a
	$(CC) -o $@ automerge.o $(LDIR)/libautomerge.a $(LIBS) -L$(LDIR)

$(DEBUG_LIB): src/*.rs
	cargo build

$(LIB): src/*.rs
	cargo build --release

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

.PHONY: clean

clean:
	rm -f *.o automerge $(LIB) $(DEBUG_LIB)
