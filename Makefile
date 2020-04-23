current_dir = $(shell pwd)

build:
	docker build -t chime/python-bug-squash .

shell: build
	docker run -v $(current_dir):/usr/src/app -it chime/python-bug-squash sh
