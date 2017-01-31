#!/usr/bin/env bash

git checkout gh-pages
rm -rf _*
touch .nojekyll
git checkout master docs hurray logo.png favicon.ico
make html -C ./docs
mv ./docs/_build/html/* ./
rm -rf ./docs
git add -A
git commit -m "publishing updated github pages..."
git push origin gh-pages

git checkout master
