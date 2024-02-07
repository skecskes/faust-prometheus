from setuptools import setup, find_packages

with open("requirements.txt", "r") as fp:
    install_requires = fp.read()

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='faust-prometheus',
    version="0.0.6",
    author='Stefan Kecskes',
    author_email='mr.kecskes@gmail.com',
    description="Prometheus metrics for Faust stream processing framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/skecskes/faust-prometheus",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Development Status :: 5 - Production/Stable",
        "Topic :: System :: Monitoring"
    ],
    install_requires=install_requires,
    zip_safe=True,
    python_requires='>=3.6'
)
