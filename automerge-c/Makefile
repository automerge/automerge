
CC=gcc
CFLAGS=-I.
DEPS = automerge.h
LIBS=-lautomerge -lpthread -ldl
LDIR=../target/debug

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

automerge: automerge.o
	$(CC) -o $@ $^ $(LIBS) -L$(LDIR)

.PHONY: clean

clean:
	rm -f *.o automerge
