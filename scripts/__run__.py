import QAR_Decode
import sys

try:
    my_QAR_Decode = QAR_Decode.initialize()

    my_QAR_Decode.QAR_Decode(
        sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], nargout=0
    )

    my_QAR_Decode.terminate()
except Exception as e:
    print("Error. Subprocess execution was terminated.", e)

