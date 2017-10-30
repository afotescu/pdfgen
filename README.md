# pdfgen

Pdfgen is a worker which checks every second if we have a pdf generation waiting in database.
If yes then we create a lot of pdfs for employees with different configuration/layouts and other stuff.

It works with [hummusjs](https://github.com/galkahana/HummusJS) a very fast library for processing pdf files written in c++ wih node bindings.