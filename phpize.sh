#!/bin/bash
# This scripts turns the Mkdocs-generated site into a fake PHP site for  Heroku
mv site/index.html site/home.html
cp php-stub/* site/
