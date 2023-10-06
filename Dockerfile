# To enable ssh & remote debugging on app service change the base image to the one below
# FROM mcr.microsoft.com/azure-functions/python:3.0-python3.7-appservice
FROM mcr.microsoft.com/windows/servercore:ltsc2019

ADD https://ssd.mathworks.com/supportfiles/downloads/R2020b/Release/8/deployment_files/installer/complete/win64/MATLAB_Runtime_R2020b_Update_8_win64.zip C:\\MCR_R2020b_win64_installer.zip

# Line 3: Use PowerShell
SHELL ["powershell", "-Command", "$ErrorActionPreference = 'Stop'; $ProgressPreference = 'SilentlyContinue';"]

# Line 4: Unpack ZIP contents to installation folder
RUN Expand-Archive C:\\MCR_R2020b_win64_installer.zip -DestinationPath C:\\MCR_INSTALLER

# Line 5: Run the setup command for a non-interactive installation of MCR
RUN Start-Process C:\MCR_INSTALLER\bin\win64\setup_legacy.exe -ArgumentList '-mode silent', '-agreeToLicense yes' -Wait

# Line 6: Remove ZIP and installation folder after setup is complete
RUN Remove-Item -Force -Recurse C:\\MCR_INSTALLER, C:\\MCR_R2020b_win64_installer.zip

RUN Invoke-WebRequest -UseBasicParsing https://www.python.org/ftp/python/3.8.9/python-3.8.9-amd64.exe -OutFile python.exe; `Start-Process python.exe -Wait -ArgumentList /PrependPath=1, /quiet; `Remove-Item -Force python.exe

RUN $env:PATH = 'C:\Users\ContainerAdministrator\AppData\Local\Programs\Python\Python38;C:\Program Files\MATLAB\MATLAB Runtime\v99\runtime\win64;C:\Users\ContainerAdministrator\AppData\Local\Programs\Python\Python38\Scripts' ;\
    [Environment]::SetEnvironmentVariable('PATH', $env:PATH, [EnvironmentVariableTarget]::Machine)

WORKDIR /app
COPY . .
SHELL ["cmd", "/S", "/C"]

ADD https://bootstrap.pypa.io/get-pip.py get-pip.py

RUN python get-pip.py && \
    pip install -r requirements.txt

ENTRYPOINT [ "python", "./scripts/__qar_decode__.py" ]