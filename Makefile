.PHONY: FORCE
.SUFFIXES:
.DELETE_ON_ERROR:

install: setup.py
	python $< install


.PHONY: clean cleaner cleanest
clean:
cleaner: clean
	/bin/rm -fr build dist syapsefdw.egg-info
cleanest: cleaner
