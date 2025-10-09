from os import system
from progress.bar import ChargingBar
import time
import requests
import json
# from pprint import pprint


class wrapper:
    token = ''
    name = ''
    account = ''
    character = {}
    cooldown = {}

    def __init__(self, account, name, token_file):
        self.account = account
        self.name = name

        with open(token_file, "r") as file:
            self.token = file.readline().rstrip()

        self.update()
        self.status()

    def _post(self, suffix, data={}):
        base_address = "https://api.artifactsmmo.com"
        address = f"{base_address}/{suffix}"
        header = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        data_json = json.dumps(data)
        response = requests.post(address, data=data_json, headers=header)
        if response.status_code != 200:
            data = response.json()
            print(f"Error {response.status_code}: {data['error']['message']}")
            return False
        else:
            data = response.json()['data']
            self.character = data['character']
            self.cooldown = data['cooldown']
            return response

    def _get(self, suffix, data={}):
        base_address = "https://api.artifactsmmo.com"
        search_terms = []
        for key in data.keys():
            if data[key] != '':
                search_terms.append(f"{key}={data[key]}")
        if len(search_terms) > 0:
            suffix = f"{suffix}?{'&'.join(search_terms)}"
        address = f"{base_address}/{suffix}"
        header = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        response = requests.get(address, headers=header)
        if response.status_code != 200:
            print(f"Error {response.status_code}: {data['error']['message']}")
            return False
        else:
            return response

    def _wait(self):
        try:
            seconds = self.cooldown['remaining_seconds']
            reason = self.cooldown['reason']
            bar = ChargingBar(
                f"{reason} cooldown ({seconds}s)", max=seconds*10)
            for i in range(seconds*10):
                time.sleep(0.1)
                bar.next()
            bar.finish()
        except KeyError:
            seconds = 0

    def update(self):
        suffix = f"characters/{self.name}"
        response = self._get(suffix)
        if response:
            data = response.json()
            self.character = data['data']

    def move(self, x, y):
        suffix = f"my/{self.name}/action/move"
        data = {"x": x, "y": y}
        response = self._post(suffix, data)
        if response:
            data = response.json()
            content = data["data"]["destination"]["content"]
            print(f"moved to {data['data']['destination']['name']}")
            image_name = f"{data['data']['destination']['skin']}.png"
            image_url = "https://www.artifactsmmo.com/"
            viu = "viu -w 16 -h 8 -"
            system(f"curl -s {image_url}images/maps/{image_name} | {viu}")
            if isinstance(content, dict):
                print(f"{content['type']}: {content['code']}")
            self._wait()

    def equip(self, code, slot):
        suffix = f"my/{self.name}/action/equip"
        data = {"code": code, "slot": slot}
        response = self._post(suffix, data)
        if response:
            data = response.json()
            itemname = data["data"]["item"]["code"]
            slotname = data["data"]["slot"]
            print(f"{itemname} equiped to {slotname}")
            self._wait()

    def unequip(self, slot):
        suffix = f"my/{self.name}/action/unequip"
        data = {"slot": slot}
        response = self._post(suffix, data)
        if response:
            data = response.json()
            itemname = data["data"]["item"]["code"]
            slotname = data["data"]["slot"]
            print(f"{itemname} unequiped from {slotname} and put in inventory")
            self._wait()

    def check_bank(self, page=1):
        suffix = "/my/bank/items"
        data = {'page': page,
                'size': 25}
        response = self._get(suffix, data)
        if response:
            data = response.json()
            bankitems = data["data"]
            print("Bank contents:")
            for item in bankitems:
                print(f"  {item['quantity']:>4} {item['code']}")
            if data['pages'] > 1:
                print(f"({data['page']}/{data['pages']})")

    def bank_deposit_item(self, code, number=1):
        suffix = f"my/{self.name}/action/bank/deposit/item"
        data = [{"code": code, "quantity": number}]
        response = self._post(suffix, data)
        if response:
            data = response.json()
            itemnum = data["data"]["items"][0]["quantity"]
            itemname = data["data"]["items"][0]["code"]
            print(f"{itemnum} {itemname} deposited in bank",)
            self._wait()

    def bank_deposit_all(self):
        suffix = f"my/{self.name}/action/bank/deposit/item"
        data = []
        for item in self.get_inventory():
            if item['quantity'] > 0:
                data.append({"code": item['code'],
                             "quantity": item['quantity']})
        if len(data) > 0:
            response = self._post(suffix, data)
            if response:
                data = response.json()
                items = data["data"]["items"]
                print("deposited:")
                for item in items:
                    print(f"  {item['quantity']:>4} {item['code']}",)
                self._wait()
        else:
            print("no items to deposit")

    def bank_withdraw_item(self, code, number=1):
        suffix = f"my/{self.name}/action/bank/withdraw/item"
        data = [{"code": code, "quantity": number}]
        response = self._post(suffix, data)
        if response:
            data = response.json()
            itemnum = data["data"]["items"][0]["quantity"]
            itemname = data["data"]["items"][0]["code"]
            print(f"{itemnum} {itemname} withdrawn from the bank",)

    def crafting(self, code, quantity=1):
        suffix = f"my/{self.name}/action/crafting"
        data = {"code": code, "quantity": quantity}
        response = self._post(suffix, data)
        if response:
            data = response.json()
            print("you crafted:")
            for item in data["data"]["details"]["items"]:
                print(f"{item['quantity']} {item['code']}")
            print(f"you gained {data['data']['details']['xp']} xp")
            self._wait()

    def fight(self):
        suffix = f"my/{self.name}/action/fight"
        response = self._post(suffix)
        if response:
            data = response.json()
            print(f"fight took {data['data']['fight']['turns']} turns")
            print(f"you earned {data['data']['fight']['xp']} xp", end='')
            gold = int(data['data']['fight']['gold'])
            if gold > 0:
                print(f" and {data['data']['fight']['gold']} gold")
            else:
                print()
            if len(data["data"]["fight"]["drops"]) > 0:
                print("loot:")
                for item in data["data"]["fight"]["drops"]:
                    print(f"{item['quantity']:>2} {item['code']}")
            self.status(showlocation=False)
            self._wait()

    def rest(self):
        suffix = f"my/{self.name}/action/rest"
        response = self._post(suffix)
        if response:
            data = response.json()
            print(f"you rested, healing {data['data']['hp_restored']} hp")
            self.status(showxp=False,
                        showlevel=False,
                        showgold=False,
                        showlocation=False)
            self._wait()

    def gathering(self):
        suffix = f"my/{self.name}/action/gathering"
        response = self._post(suffix)
        if response:
            data = response.json()
            print("you gathered:")
            for item in data["data"]["details"]["items"]:
                print(f"{item['quantity']:>2} {item['code']}")
            print(f"you gained {data['data']['details']['xp']} xp")
            self._wait()

    def new_task(self):
        suffix = f"my/{self.name}/action/task/new"
        response = self._post(suffix)
        if response:
            print("new task:")
            data = response.json()
            total = data['data']['task']['total']
            code = data['data']['task']['code']
            if data["data"]["task"]["type"] == "monsters":
                print(f"  kill {total} {code}")
            else:
                print(f"  return {total} {code}")
            print("reward:")
            if data['data']['task']['rewards']['gold'] > 0:
                print(f"  {data['data']['task']['rewards']['gold']:>3} gold")
            for item in data['data']['task']['rewards']['items']:
                print(f"  {item['quantity']:>3} {item['code']}")
            self._wait()

    def check_task(self):
        task_type = self.character['task_type']
        task_code = self.character['task']
        task_progress = self.character['task_progress']
        task_total = self.character['task_total']
        if task_type == "monsters":
            task_verb = "kill"
        else:
            task_verb = "return"
        print("current task:")
        print(f"  {task_progress}/{task_total} {task_verb} {task_code}")

    def complete_task(self):
        suffix = f"my/{self.name}/action/task/complete"
        response = self._post(suffix)
        if response:
            data = response.json()
            print("task completed\nreward:")
            if data['data']['rewards']['gold'] > 0:
                print(f"  {data['data']['rewards']['gold']:>3} gold")
            for item in data['data']['rewards']['items']:
                print(f"  {item['quantity']:>3} {item['code']}")
            self._wait()

    def get_maps(self, content_type='', content_code=''):
        suffix = "/maps"
        data = {
            'content_type': content_type,
            'content_code': content_code
        }
        response = self._get(suffix, data)
        if response:
            data = response.json()
            return data['data']

    def get_map(self, x, y):
        suffix = f"/maps/{x}/{y}"
        response = self._get(suffix)
        if response:
            data = response.json()
            return data['data']

    def get_inventory(self):
        return self.character['inventory']

    def get_inventory_space(self):
        total = self.character['inventory_max_items']
        used = 0
        for item in self.get_inventory():
            used += item['quantity']
        return total - used

    def inventory(self):
        print("Inventory:")
        output = True
        if len(self.get_inventory()) > 0:
            for item in self.get_inventory():
                if item['quantity'] > 0:
                    output = False
                    print(f"{item['quantity']:>4} {item['code']}")
        if output:
            print("  Nothing")

    def equipment(self):
        print("Equipment:")
        slots = []
        for key in self.character.keys():
            if '_slot' in key:
                slots.append(key)
        for slot in slots:
            print(f"{slot.replace('_slot', ''):>17}: {self.character[slot]}")

    def status(self, showhp=True, showxp=True,
               showlevel=True, showgold=True, showlocation=True):
        hp = self.character['hp']
        max_hp = self.character['max_hp']
        xp = self.character['xp']
        max_xp = self.character['max_xp']
        level = self.character['level']
        gold = self.character['gold']
        if showhp:
            print(f"hp: {hp}/{max_hp} ({round((100.0*hp)/max_hp, 1)}%)")
        if showxp:
            print(f"xp: {xp}/{max_xp} ({round((100.0*xp)/max_xp, 1)}%)")
        if showlevel:
            print(f"level: {level}")
        if showgold:
            print(f"gold: {gold}")
        if showlocation:
            x = self.character['x']
            y = self.character['y']
            data = self.get_map(x, y)
            if data:
                content = data["content"]
                print(f"location: {data['name']} ({x}, {y})")
                image_url = "https://www.artifactsmmo.com/"
                image_name = f"{data['skin']}.png"
                viu = "viu -w 16 -h 8 -"
                system(f"curl -s {image_url}images/maps/{image_name} | {viu}")
                if isinstance(content, dict):
                    print(f"{content['type']}: {content['code']}")


def main():
    kemika = wrapper("kemika", "kemika", "token")
    kemika.inventory()
    while True:
        text = input(">>> ")
        if text == 'quit':
            break
        elif text == 'clear':
            system('clear')
        else:
            try:
                exec(text)
            except SyntaxError:
                print("syntax error")


if __name__ == "__main__":
    main()
