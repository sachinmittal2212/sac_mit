# set base image (host OS)
FROM public.ecr.aws/lambda/python:3.8 


# set the working directory in the container
WORKDIR ${LAMBDA_TASK_ROOT}

# copy the content of the local src directory to the working directory
COPY src/ ${LAMBDA_TASK_ROOT}

# install dependencies
RUN mkdir /usr/local/share/ca-certificates

# ADD Invesco Root Certs
RUN yum -y install ca-certificates && update-ca-trust force-enable
ADD ./IVZRoot.cert /usr/local/share/ca-certificates/IVZRoot.crt 
ADD ./ivzdev.cert /usr/local/share/ca-certificates/IVZDevRoot.crt
ADD ./ivz.cert /usr/local/share/ca-certificates/IVZ.crt
RUN chmod 644 /usr/local/share/ca-certificates/IVZRoot.crt && chmod 644 /usr/local/share/ca-certificates/IVZDevRoot.crt && chmod 644 /usr/local/share/ca-certificates/IVZ.crt && update-ca-trust extract

# Expose root certs to python
ENV SSL_CERT_FILE /etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_DIR  /etc/ssl/certs/

EXPOSE 1022
EXPOSE 22


COPY requirements.txt  .

RUN yum update -y \
    && curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/mssql-release.repo \
    && yum remove unixODBC-utf16 unixODBC-utf16-devel \
    && ACCEPT_EULA=Y yum install -y msodbcsql17  \
    && ACCEPT_EULA=Y yum install -y mssql-tools \
    && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
    && source ~/.bashrc \
    && yum install -y unixODBC-devel \
    && yum install -y gcc-c++ \
    && yum clean all \
    && pip install --no-cache-dir --upgrade pip setuptools \ 
    && pip install --no-cache-dir -r requirements.txt --target "${LAMBDA_TASK_ROOT}" --use-pep517

CMD ["lambda_function.lambda_handler"]