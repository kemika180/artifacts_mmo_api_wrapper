# [Artifacts MMO](https://api.artifactsmmo.com/docs/#/) API Wrapper

Example Script:

```python
from artifacts_mmo_api_wrapper import artifacts

def main():
    character = artifacts.wrapper('account', 'character', 'token_file', True)
    if character.character['x'] != 0 or character.character['y'] != 1:
        character.move(0, 1)
    while True:
        character.fight()
        character.rest()

if __name__ == "__main__":
    main()
```
