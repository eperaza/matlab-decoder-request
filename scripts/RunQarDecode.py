#!/usr/bin/env python
"""
Sample script that uses the QAR_Decode module created using
MATLAB Compiler SDK.

Refer to the MATLAB Compiler SDK documentation for more information.
"""

from __future__ import print_function
import QAR_Decode_Parallel
import QAR_Decode
import matlab
import os

my_QAR_Decode = QAR_Decode.initialize()

absolute_path = os.path.dirname(__file__)
relative_path = "../input"
QARDirIn = os.path.join(absolute_path, relative_path)

absolute_path = os.path.dirname(__file__)
relative_path = "../output"
OutDirIn = os.path.join(absolute_path, relative_path)

airlineIn = "QTR"
tailNumberIn = "a7bsh"

my_QAR_Decode.QAR_Decode(QARDirIn, OutDirIn, airlineIn, tailNumberIn, nargout=0)

my_QAR_Decode.terminate()
