#!/usr/bin/env python
"""
Sample script that uses the QAR_Decode module created using
MATLAB Compiler SDK.

Refer to the MATLAB Compiler SDK documentation for more information.
"""

from __future__ import print_function
import QAR_Decode
import matlab

my_QAR_Decode = QAR_Decode.initialize()

QARDirIn = "C:\\FDA\\QAR737decoder\\test"
OutDirIn = "C:\\FDA\\QAR737decoder\\test"
airlineIn = "OMA"
tailNumberIn = "a4oma"
my_QAR_Decode.QAR_Decode(QARDirIn, OutDirIn, airlineIn, tailNumberIn, nargout=0)

my_QAR_Decode.terminate()
