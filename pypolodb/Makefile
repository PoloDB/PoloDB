build: setup.py
	python setup.py build

dist: setup.py
	rm -r ./dist
	rm -r ./build
	mkdir build
	python setup.py sdist bdist_wheel
