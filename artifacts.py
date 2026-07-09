from progress.bar import ChargingBar
import time
import requests
import json
import os
import logging
import builtins
from typing import Any, Callable, Optional
from requests import Response
from .db_cache import DatabaseCache

# Library logger. A NullHandler keeps messages silent unless the host app
# configures logging (the TUI routes this into its log file). Errors that were
# previously swallowed are now recorded rather than lost.
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class wrapper:
    # Deliberately class-level: a global "time of the most recent API call
    # across all character instances", read by the TUI for idle detection.
    # Do NOT move this to the instance.
    last_api_call_time = 0.0
    _CHAR_CACHE_TTL = 1.5  # seconds between automatic character refresh calls
    _REQUEST_TIMEOUT = 30  # seconds before an API request is aborted

    # Attributes assigned at runtime — in __init__, in methods, or by callers
    # such as the TUI (see bots.patch_api_for_tui). Declared here so type
    # checkers recognise them on wrapper instances.
    last_error: Optional[str] = None
    _emit: Callable[..., None]
    action_listeners: list
    _last_update_time: float
    _wait_handler: Optional[Callable[[], None]]
    _tui_stop_event: Any
    _tui_log_callback: Callable[..., None]
    _app_update_callback: Optional[Callable[..., None]]
    _tui_patched: bool

    def __init__(self, account: str, name: str, token_file: str, show_bar: bool = False,
                 base_url: str = "https://api.artifactsmmo.com", render_images: bool = True,
                 output_cb: Optional[Callable[..., None]] = None) -> None:
        self.account = account
        self.name = name
        # Output sink for all wrapper messages. Defaults to stdout; callers
        # (e.g. the TUI) inject a per-character callback so concurrent bots'
        # log lines can be attributed and routed independently.
        self._emit = output_cb if output_cb is not None else builtins.print
        # Per-instance state. Previously these were mutable class attributes,
        # so every instance shared the same character/cooldown dict until it
        # happened to reassign them — a latent multi-character aliasing bug.
        self.character: dict = {}
        self.cooldown: dict = {}
        self.show_bar = show_bar
        self.base_url = base_url.rstrip("/")
        self.render_images = render_images
        self.image_base_url = "https://www.artifactsmmo.com"

        with open(token_file, "r") as file:
            self.token = file.readline().rstrip()

        # Initialize SQLite database cache
        wrapper_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(wrapper_dir, "cache.db")
        self.cache = DatabaseCache(db_path)
        self._check_version_and_sync()
        self.activity = "Idle"
        self.action_listeners = []
        self.auto_wait = True

        if self.name:
            if self.update():
                self.status()
            else:
                self._emit(f"Warning: Character '{name}' could not be loaded. It may not exist yet.")

    def register_action_listener(self, callback):
        """Register a callback to be notified before character actions.
        Callback signature: callback(action_name: str, args: list)
        """
        if not hasattr(self, 'action_listeners'):
            self.action_listeners = []
        self.action_listeners.append(callback)

    def trigger_action_listeners(self, action_name, args):
        if not hasattr(self, 'action_listeners'):
            self.action_listeners = []
        for cb in self.action_listeners:
            try:
                cb(action_name, args)
            except Exception:
                logger.warning("action listener failed for %s", action_name, exc_info=True)

    def _check_version_and_sync(self):
        """Check server API version and drop tables on game update."""
        try:
            response = requests.get(f"{self.base_url}/", headers={"Accept": "application/json"}, timeout=5)
            if response.status_code == 200:
                current_ver = response.json().get("data", {}).get("version")
                if current_ver:
                    cached_ver = self.cache.get_version()
                    if cached_ver != current_ver:
                        self.cache.clear_cache(current_ver)
        except Exception as e:
            # Expected when offline or the API is slow — fall back to the cache.
            logger.debug("version check/sync skipped: %s", e)

    def _post(self, suffix, data={}, update_character=True):
        wrapper.last_api_call_time = time.time()
        base_address = self.base_url
        address = f"{base_address}/{suffix}"
        header = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        data_json = json.dumps(data)
        
        while True:
            # 429 rate limit backoff retry
            retries = 3
            response = None
            for attempt in range(retries):
                try:
                    response = requests.post(address, data=data_json, headers=header, timeout=self._REQUEST_TIMEOUT)
                except requests.RequestException as e:
                    logger.warning("POST %s failed (attempt %d/%d): %s", suffix, attempt + 1, retries, e)
                    response = None
                    time.sleep(1.0)
                    continue
                if response.status_code == 429:
                    time.sleep(1.0)
                    continue
                break

            if response and response.status_code == 499:
                try:
                    err_data = response.json().get('error', {}).get('data', {})
                    if 'cooldown' in err_data:
                        self.cooldown = err_data['cooldown']
                        self._wait()
                        continue
                except Exception as e:
                    logger.debug("failed to parse 499 cooldown response: %s", e)
            break

        if response is None:
            self.last_error = "Error: No response from server"
            self._emit(self.last_error)
            return False
        if response.status_code != 200:
            try:
                data = response.json()
                self.last_error = f"Error {response.status_code}: {data['error']['message']}"
                self._emit(self.last_error)
            except Exception:
                self.last_error = f"Error {response.status_code}"
                self._emit(self.last_error)
            return False
        else:
            self.last_error = None
            if update_character:
                data = response.json()['data']
                if 'characters' in data.keys():
                    self.character = data['characters'][0]
                elif 'character' in data.keys():
                    self.character = data['character']
                if 'cooldown' in data.keys():
                    self.cooldown = data['cooldown']
            
            # Wait for cooldown to expire if auto_wait is enabled
            if getattr(self, "auto_wait", True):
                self._wait()
                
            return response

    def _get(self, suffix, data={}):
        wrapper.last_api_call_time = time.time()
        base_address = self.base_url
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
        
        # 429 rate limit backoff retry
        retries = 3
        response = None
        for attempt in range(retries):
            try:
                response = requests.get(address, headers=header, timeout=self._REQUEST_TIMEOUT)
            except requests.RequestException as e:
                logger.warning("GET %s failed (attempt %d/%d): %s", suffix, attempt + 1, retries, e)
                response = None
                time.sleep(1.0)
                continue
            if response.status_code == 429:
                time.sleep(1.0)
                continue
            break

        if response is None:
            self._emit("Error: No response from server")
            return False
        if response.status_code != 200:
            try:
                data = response.json()
                self._emit(f"Error {response.status_code}: {data['error']['message']}")
            except Exception:
                self._emit(f"Error {response.status_code}")
            return False
        else:
            return response

    def _wait(self):
        if not getattr(self, "auto_wait", True):
            return

        wait_handler = getattr(self, "_wait_handler", None)
        if wait_handler:
            wait_handler()
        else:
            try:
                seconds = self.cooldown['remaining_seconds']
                reason = self.cooldown['reason']
                if self.show_bar:
                    bar = ChargingBar(
                        f"{reason} cooldown ({seconds}s)", max=seconds*10)
                    for _ in range(seconds*10):
                        time.sleep(0.1)
                        bar.next()
                    bar.finish()
                else:
                    time.sleep(seconds)
            except KeyError:
                pass

        # The cooldown has now elapsed. Mark it consumed so a redundant second
        # _wait() — the action methods each call _wait() after _post has
        # already waited — doesn't sleep the same cooldown again. The next
        # action refreshes self.cooldown from the server response.
        if isinstance(self.cooldown, dict):
            self.cooldown['remaining_seconds'] = 0

    def update(self, force: bool = False) -> bool:
        """Refresh character state from the API.

        Uses a short TTL cache to avoid hammering the API when conditions
        or expressions are evaluated rapidly (e.g. inside a script loop).
        Pass force=True to bypass the cache and always fetch fresh data.
        Note: _post() already updates self.character from the action response,
        so force is rarely needed outside of explicit polling contexts.
        """
        now = time.time()
        if not force and (now - getattr(self, '_last_update_time', 0.0)) < self._CHAR_CACHE_TTL:
            return bool(self.character)
        suffix = f"characters/{self.name}"
        response = self._get(suffix)
        if response:
            data = response.json()
            self.character = data['data']
            self._last_update_time = now
            return True
        return False

    def _render_map_image(self, skin):
        """Best-effort inline render of a map skin image in the terminal.

        Fetches the PNG over HTTPS and renders it with textual-image's Rich
        renderable, which auto-selects the best available protocol (Kitty/TGP,
        Sixel, or a unicode fallback) — the same library the TUI uses, so
        there is no external binary or shell involved.

        The rendering deps (rich, textual-image) are imported lazily so this
        API wrapper stays lightweight for consumers that don't want them.
        No-ops silently if rendering is disabled, the deps are absent, or the
        fetch fails — map art is non-essential.
        """
        if not self.render_images or not skin:
            return
        try:
            import io
            from rich.console import Console
            from textual_image.renderable import Image as RenderableImage
        except ImportError:
            return  # optional rendering deps not installed
        url = f"{self.image_base_url}/images/maps/{skin}.png"
        try:
            resp = requests.get(url, timeout=self._REQUEST_TIMEOUT)
            if resp.status_code != 200:
                return
            Console().print(RenderableImage(io.BytesIO(resp.content), width=16, height=8))
        except requests.RequestException:
            pass  # network/timeout — art is non-essential

    def move(self, x: int, y: int) -> None:
        self.trigger_action_listeners("move", [x, y])
        suffix = f"my/{self.name}/action/move"
        data = {"x": x, "y": y}
        response = self._post(suffix, data)
        if response:
            data = response.json()
            content = data["data"]["destination"]["interactions"]["content"]
            self._emit(f"moved to {data['data']['destination']['name']}")
            self._render_map_image(data['data']['destination']['skin'])
            if isinstance(content, dict):
                self._emit(f"{content['type']}: {content['code']}")
            self._wait()

    def equip(self, code, slot):
        self.trigger_action_listeners("equip", [code, slot])
        suffix = f"my/{self.name}/action/equip"
        # API requires a list of items to equip
        data = [{"code": code, "slot": slot}]
        response = self._post(suffix, data)
        if response:
            data = response.json()
            items = data["data"].get("items", [])
            if items:
                itemname = items[0]["item"]["code"]
                slotname = items[0]["slot"]
                self._emit(f"{itemname} equiped to {slotname}")
            self._wait()
            return response
        return False

    def unequip(self, slot):
        self.trigger_action_listeners("unequip", [slot])
        suffix = f"my/{self.name}/action/unequip"
        # API requires a list of items to unequip
        data = [{"slot": slot}]
        response = self._post(suffix, data)
        if response:
            data = response.json()
            items = data["data"].get("items", [])
            if items:
                itemname = items[0]["item"]["code"]
                slotname = items[0]["slot"]
                self._emit(f"{itemname} unequiped from {slotname} and put in inventory")
            self._wait()
            return response
        return False

    def check_bank(self, page=1):
        suffix = "/my/bank/items"
        data = {'page': page,
                'size': 25}
        response = self._get(suffix, data)
        if response:
            data = response.json()
            bankitems = data["data"]
            self._emit("Bank contents:")
            for item in bankitems:
                self._emit(f"  {item['quantity']:>4} {item['code']}")
            if data['pages'] > 1:
                self._emit(f"({data['page']}/{data['pages']})")
            return bankitems
        return []

    def bank_deposit_item(self, code: str, number: int = 1) -> None:
        self.trigger_action_listeners("bank_deposit_item", [code, number])
        suffix = f"my/{self.name}/action/bank/deposit/item"
        data = [{"code": code, "quantity": number}]
        response = self._post(suffix, data)
        if response:
            data = response.json()
            itemnum = data["data"]["items"][0]["quantity"]
            itemname = data["data"]["items"][0]["code"]
            self._emit(f"{itemnum} {itemname} deposited in bank",)
            self._wait()

    def bank_deposit_all(self) -> None:
        self.trigger_action_listeners("bank_deposit_all", [])
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
                self._emit("deposited:")
                for item in items:
                    self._emit(f"  {item['quantity']:>4} {item['code']}",)
                self._wait()
        else:
            self._emit("no items to deposit")

    def bank_withdraw_item(self, code: str, number: int = 1) -> None:
        self.trigger_action_listeners("bank_withdraw_item", [code, number])
        suffix = f"my/{self.name}/action/bank/withdraw/item"
        data = [{"code": code, "quantity": number}]
        response = self._post(suffix, data)
        if response:
            data = response.json()
            itemnum = data["data"]["items"][0]["quantity"]
            itemname = data["data"]["items"][0]["code"]
            self._emit(f"{itemnum} {itemname} withdrawn from the bank",)
            self._wait()

    def crafting(self, code: str, quantity: int = 1) -> None:
        self.trigger_action_listeners("crafting", [code, quantity])
        suffix = f"my/{self.name}/action/crafting"
        data = {"code": code, "quantity": quantity}
        response = self._post(suffix, data)
        if response:
            data = response.json()
            self._emit("you crafted:")
            for item in data["data"]["details"]["items"]:
                self._emit(f"{item['quantity']} {item['code']}")
            self._emit(f"you gained {data['data']['details']['xp']} xp")
            self._wait()

    def fight(self) -> None:
        self.trigger_action_listeners("fight", [])
        suffix = f"my/{self.name}/action/fight"
        response = self._post(suffix)
        if response:
            data = response.json()
            self._emit(f"fight took {data['data']['fight']['turns']} turns")
            self._emit(f"you earned {data['data']['fight']['characters'][0]['xp']} xp", end='')
            gold = int(data['data']['fight']['characters'][0]['gold'])
            if gold > 0:
                self._emit(f" and {gold} gold")
            else:
                self._emit()
            drops = data['data']['fight']['characters'][0]['drops']
            if len(drops) > 0:
                self._emit("loot:")
                for item in drops:
                    self._emit(f"{item['quantity']:>2} {item['code']}")
            self.status(showlocation=False)
            self._wait()

    def rest(self) -> None:
        self.trigger_action_listeners("rest", [])
        suffix = f"my/{self.name}/action/rest"
        response = self._post(suffix)
        if response:
            data = response.json()
            self._emit(f"you rested, healing {data['data']['hp_restored']} hp")
            self.status(showxp=False,
                        showlevel=False,
                        showgold=False,
                        showlocation=False)
            self._wait()

    def gathering(self) -> None:
        self.trigger_action_listeners("gathering", [])
        suffix = f"my/{self.name}/action/gathering"
        response = self._post(suffix)
        if response:
            data = response.json()
            self._emit("you gathered:")
            for item in data["data"]["details"]["items"]:
                self._emit(f"{item['quantity']:>2} {item['code']}")
            self._emit(f"you gained {data['data']['details']['xp']} xp")
            self._wait()

    def new_task(self):
        suffix = f"my/{self.name}/action/task/new"
        response = self._post(suffix)
        if response:
            self._emit("new task:")
            data = response.json()
            total = data['data']['task']['total']
            code = data['data']['task']['code']
            if data["data"]["task"]["type"] == "monsters":
                self._emit(f"  kill {total} {code}")
            else:
                self._emit(f"  return {total} {code}")
            self._emit("reward:")
            if data['data']['task']['rewards']['gold'] > 0:
                self._emit(f"  {data['data']['task']['rewards']['gold']:>3} gold")
            for item in data['data']['task']['rewards']['items']:
                self._emit(f"  {item['quantity']:>3} {item['code']}")
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
        self._emit("current task:")
        self._emit(f"  {task_progress}/{task_total} {task_verb} {task_code}")

    def complete_task(self):
        self.trigger_action_listeners("complete_task", [])
        suffix = f"my/{self.name}/action/task/complete"
        response = self._post(suffix)
        if response:
            data = response.json()
            self._emit("task completed\nreward:")
            if data['data']['rewards']['gold'] > 0:
                self._emit(f"  {data['data']['rewards']['gold']:>3} gold")
            for item in data['data']['rewards']['items']:
                self._emit(f"  {item['quantity']:>3} {item['code']}")
            self._wait()

    def get_maps(self, content_type: str = '', content_code: str = '',
                 hide_blocked_maps: bool = True, layer: str = '') -> Optional[list]:
        query_key = f"{content_type}:{content_code}:{hide_blocked_maps}:{layer}"
        cached = self.cache.get_maps(query_key)
        if cached is not None:
            return cached

        suffix = "/maps"
        data = {
            'content_type': content_type,
            'content_code': content_code,
            'hide_blocked_maps': hide_blocked_maps,
            'layer': layer
        }
        response = self._get(suffix, data)
        if response:
            data = response.json()['data']
            self.cache.set_maps(query_key, data)
            return data
        return None

    def get_map(self, x: int, y: int, layer: str) -> Optional[dict]:
        query_key = f"single:{layer}:{x}:{y}"
        cached = self.cache.get_maps(query_key)
        if cached is not None:
            return cached

        suffix = f"/maps/{layer}/{x}/{y}"
        response = self._get(suffix)
        if response:
            data = response.json().get('data')
            if data:
                self.cache.set_maps(query_key, data)
                return data
        return None

    def get_inventory(self) -> list:
        return self.character['inventory']

    def get_inventory_space(self) -> int:
        total = self.character['inventory_max_items']
        used = 0
        for item in self.get_inventory():
            used += item['quantity']
        return total - used

    def inventory(self):
        self._emit("Inventory:")
        output = True
        if len(self.get_inventory()) > 0:
            for item in self.get_inventory():
                if item['quantity'] > 0:
                    output = False
                    self._emit(f"{item['quantity']:>4} {item['code']}")
        if output:
            self._emit("  Nothing")

    def equipment(self):
        self._emit("Equipment:")
        slots = []
        for key in self.character.keys():
            if '_slot' in key:
                slots.append(key)
        for slot in slots:
            self._emit(f"{slot.replace('_slot', ''):>17}: {self.character[slot]}")

    def status(self, showhp=True, showxp=True,
               showlevel=True, showgold=True, showlocation=True):
        if not self.character:
            self._emit("No character data loaded.")
            return
        hp = self.character['hp']
        max_hp = self.character['max_hp']
        xp = self.character['xp']
        max_xp = self.character['max_xp']
        level = self.character['level']
        gold = self.character['gold']
        if showhp:
            self._emit(f"hp: {hp}/{max_hp} ({round((100.0*hp)/max_hp, 1)}%)")
        if showxp:
            self._emit(f"xp: {xp}/{max_xp} ({round((100.0*xp)/max_xp, 1)}%)")
        if showlevel:
            self._emit(f"level: {level}")
        if showgold:
            self._emit(f"gold: {gold}")
        if showlocation:
            x = self.character['x']
            y = self.character['y']
            layer = self.character['layer']
            data = self.get_map(x, y, layer)
            if data:
                content = data["interactions"]["content"]
                self._emit(f"location: {data['name']} ({x}, {y})")
                self._render_map_image(data['skin'])
                if isinstance(content, dict):
                    self._emit(f"{content['type']}: {content['code']}")

    def use_item(self, code: str, quantity: int = 1) -> None:
        self.trigger_action_listeners("use_item", [code, quantity])
        suffix = f"my/{self.name}/action/use"
        data = {"code": code, "quantity": quantity}
        response = self._post(suffix, data)
        if response:
            self._emit(f"used {quantity} {code}")
            self._wait()

    def bank_deposit_gold(self, quantity):
        suffix = f"my/{self.name}/action/bank/deposit/gold"
        data = {"quantity": quantity}
        response = self._post(suffix, data)
        if response:
            self._emit(f"deposited {quantity} gold to bank")
            self._wait()

    def bank_withdraw_gold(self, quantity):
        suffix = f"my/{self.name}/action/bank/withdraw/gold"
        data = {"quantity": quantity}
        response = self._post(suffix, data)
        if response:
            self._emit(f"withdrew {quantity} gold from bank")
            self._wait()

    def bank_buy_expansion(self):
        suffix = f"my/{self.name}/action/bank/buy_expansion"
        response = self._post(suffix)
        if response:
            self._emit("purchased bank expansion")
            self._wait()

    def buy_npc(self, code: str, quantity: int = 1) -> None:
        suffix = f"my/{self.name}/action/npc/buy"
        data = {"code": code, "quantity": quantity}
        response = self._post(suffix, data)
        if response:
            self._emit(f"bought {quantity} {code} from NPC merchant")
            self._wait()

    def sell_npc(self, code: str, quantity: int = 1) -> None:
        suffix = f"my/{self.name}/action/npc/sell"
        data = {"code": code, "quantity": quantity}
        response = self._post(suffix, data)
        if response:
            self._emit(f"sold {quantity} {code} to NPC merchant")
            self._wait()

    def recycle_item(self, code: str, quantity: int = 1) -> None:
        self.trigger_action_listeners("recycle_item", [code, quantity])
        suffix = f"my/{self.name}/action/recycling"
        data = {"code": code, "quantity": quantity}
        response = self._post(suffix, data)
        if response:
            self._emit(f"recycled {quantity} {code}")
            self._wait()

    def cancel_task(self):
        suffix = f"my/{self.name}/action/task/cancel"
        response = self._post(suffix)
        if response:
            self._emit("canceled task")
            self._wait()

    def exchange_task(self):
        suffix = f"my/{self.name}/action/task/exchange"
        response = self._post(suffix)
        if response:
            self._emit("exchanged task rewards")
            self._wait()

    def trade_task(self, code: str, quantity: int = 1) -> None:
        suffix = f"my/{self.name}/action/task/trade"
        data = {"code": code, "quantity": quantity}
        response = self._post(suffix, data)
        if response:
            self._emit(f"traded task items: {quantity} {code}")
            self._wait()

    def ge_buy(self, id, quantity):
        self.trigger_action_listeners("ge_buy", [id, quantity])
        suffix = f"my/{self.name}/action/grandexchange/buy"
        data = {"id": id, "quantity": quantity}
        response = self._post(suffix, data)
        if response:
            self._emit(f"purchased {quantity} items from Grand Exchange order {id}")
            self._wait()

    def ge_create_sell_order(self, code, quantity, price):
        self.trigger_action_listeners("ge_create_sell_order", [code, quantity, price])
        suffix = f"my/{self.name}/action/grandexchange/create_sell_order"
        data = {"code": code, "quantity": quantity, "price": price}
        response = self._post(suffix, data)
        if response:
            self._emit(f"created GE sell order for {quantity} {code} @ {price} gold each")
            self._wait()

    def ge_create_buy_order(self, code, quantity, price):
        self.trigger_action_listeners("ge_create_buy_order", [code, quantity, price])
        suffix = f"my/{self.name}/action/grandexchange/create_buy_order"
        data = {"code": code, "quantity": quantity, "price": price}
        response = self._post(suffix, data)
        if response:
            self._emit(f"created GE buy order for {quantity} {code} @ {price} gold each")
            self._wait()

    def ge_cancel_order(self, id):
        self.trigger_action_listeners("ge_cancel_order", [id])
        suffix = f"my/{self.name}/action/grandexchange/cancel"
        data = {"id": id}
        response = self._post(suffix, data)
        if response:
            self._emit(f"canceled GE order {id}")
            self._wait()

    def change_skin(self, skin):
        suffix = f"my/{self.name}/action/change_skin"
        data = {"skin": skin}
        response = self._post(suffix, data)
        if response:
            self._emit(f"changed skin to {skin}")
            self._wait()

    def give_item(self, character_name, code, quantity):
        suffix = f"my/{self.name}/action/give/item"
        data = {
            "character": character_name,
            "items": [{"code": code, "quantity": quantity}]
        }
        response = self._post(suffix, data)
        if response:
            self._emit(f"gave {quantity} {code} to character {character_name}")
            self._wait()

    def give_gold(self, character_name, quantity):
        suffix = f"my/{self.name}/action/give/gold"
        data = {"character": character_name, "quantity": quantity}
        response = self._post(suffix, data)
        if response:
            self._emit(f"gave {quantity} gold to character {character_name}")
            self._wait()

    def transition_layer(self):
        suffix = f"my/{self.name}/action/transition"
        response = self._post(suffix)
        if response:
            self._emit(f"transitioned map layer")
            self._wait()

    def delete_item(self, code, quantity=1):
        self.trigger_action_listeners("delete_item", [code, quantity])
        suffix = f"my/{self.name}/action/delete"
        data = {"code": code, "quantity": quantity}
        response = self._post(suffix, data)
        if response:
            self._emit(f"deleted item: {quantity} {code}")
            self._wait()

    def get_pending_items(self, page=1, size=50):
        suffix = "my/pending_items"
        data = {"page": page, "size": size}
        response = self._get(suffix, data)
        if response:
            return response.json()['data']
        return []

    def claim_item(self, id):
        self.trigger_action_listeners("claim_item", [id])
        suffix = f"my/{self.name}/action/claim_item/{id}"
        response = self._post(suffix)
        if response:
            self._emit(f"claimed pending item: {id}")
            self._wait()

    def ge_fill(self, id, quantity):
        self.trigger_action_listeners("ge_fill", [id, quantity])
        suffix = f"my/{self.name}/action/grandexchange/fill"
        data = {"id": id, "quantity": quantity}
        response = self._post(suffix, data)
        if response:
            self._emit(f"sold {quantity} items to GE buy order {id}")
            self._wait()

    def get_my_characters(self):
        suffix = "my/characters"
        response = self._get(suffix)
        if response:
            return response.json()['data']
        return []

    def create_character(self, name, skin):
        suffix = "characters/create"
        data = {"name": name, "skin": skin}
        response = self._post(suffix, data, update_character=False)
        if response:
            self._emit(f"Character {name} created successfully.")
            return True
        return False

    def delete_character(self, name):
        suffix = "characters/delete"
        data = {"name": name}
        response = self._post(suffix, data, update_character=False)
        if response:
            self._emit(f"Character {name} deleted successfully.")
            return True
        return False

    def get_item(self, code: str) -> Optional[dict]:
        cached = self.cache.get_item(code)
        if cached:
            return cached
        suffix = f"items/{code}"
        response = self._get(suffix)
        if response:
            data = response.json()['data']
            self.cache.set_item(code, data)
            return data
        return None

    def get_craft_recipe(self, code: str) -> Optional[dict]:
        item = self.get_item(code)
        if item and item.get('craft'):
            return item['craft']
        return None

    def simulate_fight(self, monster_code, fake_characters, iterations=100):
        suffix = "simulation/fight"
        data = {
            "monster": monster_code,
            "characters": fake_characters,
            "iterations": iterations
        }
        response = self._post(suffix, data, update_character=False)
        if response:
            return response.json()['data']
        return None

    def get_character_as_fake(self):
        """Converts the current character stats/gear to a FakeCharacterSchema dictionary."""
        if not self.character:
            return None
        slots = [
            'weapon_slot', 'shield_slot', 'helmet_slot', 'body_armor_slot',
            'leg_armor_slot', 'boots_slot', 'ring1_slot', 'ring2_slot',
            'amulet_slot', 'artifact1_slot', 'artifact2_slot', 'artifact3_slot',
            'utility1_slot', 'utility2_slot'
        ]
        fake_char = {
            "level": self.character.get('level', 1)
        }
        for slot in slots:
            val = self.character.get(slot)
            if val:
                fake_char[slot] = val
        fake_char['utility1_slot_quantity'] = self.character.get('utility1_slot_quantity', 1)
        fake_char['utility2_slot_quantity'] = self.character.get('utility2_slot_quantity', 1)
        return fake_char

    def simulate_self_fight(self, monster_code: str, iterations: int = 100) -> Optional[dict]:
        fake_char = self.get_character_as_fake()
        if not fake_char:
            self._emit("Error: No character loaded to simulate.")
            return None
        return self.simulate_fight(monster_code, [fake_char], iterations)

    def get_monster(self, code: str) -> Optional[dict]:
        """Retrieves details of a specific monster (HP, attack, defense, weakness, drops)."""
        cached = self.cache.get_monster(code)
        if cached:
            return cached
        suffix = f"monsters/{code}"
        response = self._get(suffix)
        if response:
            data = response.json()['data']
            self.cache.set_monster(code, data)
            return data
        return None

    def get_resource(self, code: str) -> Optional[dict]:
        """Retrieves details of a specific resource (skills required, drop rates)."""
        cached = self.cache.get_resource(code)
        if cached:
            return cached
        suffix = f"resources/{code}"
        response = self._get(suffix)
        if response:
            data = response.json()['data']
            self.cache.set_resource(code, data)
            return data
        return None

    def get_active_events(self):
        """Retrieves currently active world events on the map."""
        suffix = "events/active"
        response = self._get(suffix)
        if response:
            return response.json()['data']
        return []

    def get_ge_orders(self, code=None, type=None, page=1, size=20):
        """Retrieves active Grand Exchange orders with optional filters."""
        suffix = "grandexchange/orders"
        data = {
            "page": page,
            "size": size
        }
        if code:
            data["code"] = code
        if type:
            data["type"] = type
        response = self._get(suffix, data)
        if response:
            return response.json()['data']
        return []

    def get_items(self, page=1, size=20):
        """Retrieves a paginated list of all items in the game."""
        suffix = "items"
        data = {"page": page, "size": size}
        response = self._get(suffix, data)
        if response:
            return response.json()['data']
        return []

    def get_monsters(self, page=1, size=20):
        """Retrieves a paginated list of all monsters in the game."""
        suffix = "monsters"
        data = {"page": page, "size": size}
        response = self._get(suffix, data)
        if response:
            return response.json()['data']
        return []

    def get_resources(self, page=1, size=20):
        """Retrieves a paginated list of all harvesting resources in the game."""
        suffix = "resources"
        data = {"page": page, "size": size}
        response = self._get(suffix, data)
        if response:
            return response.json()['data']
        return []

    def get_tasks_list(self, page=1, size=20):
        """Retrieves available tasks list from Task Master."""
        suffix = "tasks/list"
        data = {"page": page, "size": size}
        response = self._get(suffix, data)
        if response:
            return response.json()['data']
        return []

    def get_task_rewards(self, page=1, size=20):
        """Retrieves the list of possible task rewards."""
        suffix = "tasks/rewards"
        data = {"page": page, "size": size}
        response = self._get(suffix, data)
        if response:
            return response.json()['data']
        return []

    def get_other_character(self, name):
        """Retrieves information of another player's character."""
        suffix = f"characters/{name}"
        response = self._get(suffix)
        if response:
            return response.json()['data']
        return None

    def get_character_leaderboard(self, page=1, size=20):
        """Retrieves character leaderboards (highest level characters)."""
        suffix = "leaderboard/characters"
        data = {"page": page, "size": size}
        response = self._get(suffix, data)
        if response:
            return response.json()['data']
        return []

    def get_all_craftable_items(self):
        """Returns a list of all craftable items in the game, syncing from the server if cache is empty."""
        conn = self.cache._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM items")
            count = cursor.fetchone()[0]
        except Exception as e:
            logger.debug("items count query failed: %s", e)
            count = 0

        # If cache is nearly empty, sync items from the server
        if count < 100:
            page = 1
            while True:
                items_page = self.get_items(page=page, size=100)
                if not items_page:
                    break
                for item in items_page:
                    self.cache.set_item(item['code'], item)
                if len(items_page) < 100:
                    break
                page += 1

        try:
            cursor.execute("SELECT data FROM items")
            rows = cursor.fetchall()
        except Exception as e:
            logger.debug("items fetch query failed: %s", e)
            rows = []
        conn.close()

        craftables = []
        for row in rows:
            try:
                item = json.loads(row[0])
                if item.get("craft"):
                    craftables.append(item)
            except Exception as e:
                logger.debug("skipping unparseable item row: %s", e)
        return craftables

def main():
    builtins.print("This script does not support being run directly. You should import it into a project and access the functions from there.")

if __name__ == "__main__":
    main()
