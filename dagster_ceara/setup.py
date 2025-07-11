from setuptools import find_packages, setup

setup(
    name="dagster_ceara",
    version="0.1.0",
    author="Pipeline IBGE Ceará",
    description="Pipeline de dados populacionais do Ceará usando API SIDRA do IBGE",
    packages=find_packages(exclude=["dagster_ceara_tests"]),
    install_requires=[
        "dagster>=1.5.0",
        "dagster-webserver",
        "pandas>=1.5.0",
        "requests>=2.28.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=0.991",
        ]
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
