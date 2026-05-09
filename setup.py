from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="kafka-http-proxy",
    version="1.0.0",
    author="Vladimir Fedorov",
    author_email="sin@krasno.ru",
    description="Synchronous HTTP to Kafka proxy with request-reply pattern",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/fedorovvl/kafka-http-proxy",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "kafka-http-proxy=kafka_http_proxy.entrypoint:main",
            "kafka-proxy=kafka_http_proxy.proxy.service:main",
            "kafka-processor=kafka_http_proxy.processor.service:main",
        ],
    },
)