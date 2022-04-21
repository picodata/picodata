## Integration testing with pytest

### Installation

1. Install Python 3.10

   **Ubuntu:**

   ```
   sudo add-apt-repository ppa:deadsnakes/ppa
   sudo apt install python3.10
   curl -sSL https://bootstrap.pypa.io/get-pip.py -o get-pip.py
   python3.10 get-pip.py
   ```

3. Install pipenv:

    ```
    python3.10 -m pip install pipenv==2022.4.8
    ```

4. Install dependencies

    ```
    python3.10 -m pipenv install --deploy
    ```

### Adding dependencies

```bash
python3.10 -m pipenv install <dependency-package-name>
```

### Running

```bash
python3.10 -m pipenv  run pytest
```

or

```bash
python3.10 -m pipenv shell
# A new shell will be opened inside the pipenv environment
pytest
```

#### Running tests in parallel with pytest-xdist

```bash
python3.10 -m pipenv run pytest -n 20
```
