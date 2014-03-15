# Lego Web Services

Get it? Go, Lego?

Basically a Golang system to help you run your own personal textfile sharing
system, or URL shortener, or whatever else you want to run.

Built on top of martini, designed to easily add/remove services.

## Git Hooks

After cloning this repo, please run:

`cd .git/hooks && ln -s ../../hooks/pre-commit .`

The pre-commit hook runs go test and ensures that the project builds before you
are allowed to make a commit. It also makes sure all files are compliant with
formatting standards using `go fmt`.
