# Contributor Guide

This package is in its early stages of development. All contributions are welcome.
You can help out just by using intake-virtual-icechunk and reporting
[issues](https://github.com/ACCESS-NRI/intake-virtual-icechunk/issues).

The following sections cover some general guidelines for maintainers and
contributors wanting to help develop intake-virtual-icechunk.

## Feature requests, suggestions and bug reports

We are eager to hear about any bugs you have found, new features you would like to see,
and any other suggestions you may have. Please feel free to submit these as
[issues](https://github.com/ACCESS-NRI/intake-virtual-icechunk/issues).

When suggesting features, please make sure to explain in detail how the proposed feature
should work and to keep the scope as narrow as possible. This makes features easier to
implement in small PRs.

When reporting bugs, please include:

- Any details about your local setup that might be helpful in troubleshooting,
  specifically the Python interpreter version, installed libraries, and
  intake-virtual-icechunk version.
- Detailed steps to reproduce the bug, ideally a
  [Minimal, Complete and Verifiable Example](http://matthewrocklin.com/blog/work/2018/02/28/minimal-bug-reports).
- If possible, a demonstration test that currently fails but should pass when the bug
  is fixed.

## Writing documentation

Adding documentation is always helpful. This may include:

- More complementary documentation. Have you perhaps found something unclear?
- Docstrings.
- Example notebooks of intake-virtual-icechunk being used in real analyses.

The intake-virtual-icechunk documentation is written in Markdown (via MyST). To build the
docs locally:

```bash
cd docs/
make html
```

This will build the documentation in `docs/_build/`. Open `_build/html/index.html` in
your web browser to view it.

## Preparing pull requests

1. Fork the
   [intake-virtual-icechunk GitHub repository](https://github.com/ACCESS-NRI/intake-virtual-icechunk).

2. Clone your fork locally and create a branch to work on:

   ```bash
   git clone git@github.com:YOUR_GITHUB_USERNAME/intake-virtual-icechunk.git
   cd intake-virtual-icechunk
   git remote add upstream git@github.com:ACCESS-NRI/intake-virtual-icechunk.git
   git checkout -b your-bugfix-feature-branch-name main
   ```

3. Install dependencies using pixi:

   ```bash
   pixi install -e test-py312
   ```

4. Install intake-virtual-icechunk in editable mode:

   ```bash
   pip install --no-deps -e .
   ```

5. Start making your edits. It can be useful to run
   [pre-commit](https://pre-commit.com) as you work:

   ```bash
   pre-commit run --all-files
   ```

6. Break your edits up into reasonably sized commits:

   ```bash
   git commit -a -m "<commit message>"
   git push -u
   ```

7. Run the tests (including those you add to test your edits!):

   ```bash
   pixi run -e test-py312 test
   ```

8. Add a new entry describing your contribution to `CHANGELOG.md`.

9. Submit a pull request through the GitHub
   [website](https://github.com/ACCESS-NRI/intake-virtual-icechunk/pulls).

## Preparing a new release

New releases to PyPI and conda are published automatically when a tag is pushed to
GitHub. The preferred approach is to trigger this by creating a new tag and corresponding
release on GitHub.
