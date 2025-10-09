# [Artifacts MMO](https://api.artifactsmmo.com/docs/#/) API Wrapper

Example Script:

```python
def main():
    character = artifacts.wrapper('account', 'character', 'token_file', True)
    character.update()
    character.move(0, 1)
    while True:
        character.fight()
        character.rest()

if __name__ == "__main__":
    main()
```
