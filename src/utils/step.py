from rich.console import Console
from rich.traceback import install as prettify_trackstack
import inspect
import re
from datetime import date
from collections import defaultdict


prettify_trackstack()
console = Console(log_path=False, record=True)


def var_name(var):
    lcls = inspect.stack()[2][0].f_locals
    for name in lcls:
        if id(var) == id(lcls[name]):
            return name
    return None


def console_log_saved(path, name=None):
    console.log(f'[green]{name or "File"} saved to "{path}"[/green]')


def console_list(items, prefix='[red]- ', head=3):
    """
    pass `head=None` to show the full list
    """
    if isinstance(items, set) or isinstance(items, tuple):
        items = list(items)
    assert isinstance(items, list)
    for item in items[:head]:
        console.log(f'{prefix}{item}')
    if head and len(items) > head:
        console.log(f'{prefix} ... and {len(items)-head} more')


def console_dict(map, prefix='[red]- ', head=3):
    c = defaultdict(set)
    for id_, value in map.items():
        c[value].add(id_)
    for value, id_set in c.items():
        console.log('[red]'+value)
        console_list(id_set, prefix=prefix, head=head)


# TODO: add 1 per call, instead of using index
class Step:
    def __init__(self, name):
        self.name = name
        self.occurances = None
        self.step = 1
        self.total = None
        self.caller_line_count = None
        self.warned = {x: False for x in ['NO_OCCURANCES']}
        console.log(f'[yellow]<{name}> starts![/yellow]')
        console.log(f'Today is {date.today().strftime("%Y-%m-%d")}')

    def step_count(self, caller, name):
        with open(caller.filename, encoding='utf-8') as f:
            content = f.read()
        self.occurances = []
        step = 1
        lines = content.split('\n')
        self.caller_line_count = len(lines)
        for li, line in enumerate(lines):
            pattern = f'{name}\(.*?\)'
            if re.search(f'[# ]{pattern}|^{pattern}', line):
                self.occurances.append([li+1, line.strip()])
                step += 1
        self.total = len(self.occurances)

    def __call__(self, msg):
        caller = inspect.stack()[1]
        if not self.occurances:
            name = var_name(self)
            self.step_count(caller, name)
        self.log_commented(caller.lineno)
        self._log(self.step, msg)
        self.step += 1

    def log_commented(self, lineno):
        if not self.occurances:
            return
        if self.step == len(self.occurances) + 1:
            return
        occurance = self.occurances[self.step-1]
        while occurance[0] != lineno:
            self._log(self.step, occurance[1], True)
            self.step += 1
            if self.step == len(self.occurances) + 1:
                return
            occurance = self.occurances[self.step-1]

    def finish(self, save_to=None):
        self.log_commented(self.caller_line_count + 2)
        console.log(f'[yellow]<{self.name}> finished!')
        if save_to:
            if save_to.endswith('.html') or save_to.endswith('.htm'):
                console.save_html(save_to)
            else:
                console.save_text(save_to)
            console_log_saved(save_to, 'Log file')

    def _log(self, index, msg, omit=False):
        if omit:
            console.log(f'[dim white]-> Step ({index}/{self.total}):  {msg}[/dim white]')
        else:
            console.log(f'[yellow]-> Step ({index}/{self.total}):[/yellow]  {msg}')

    def log(self, *args, **kwargs):
        console.log(*args, **kwargs)

    def sep(self):
        console.log('-' * 30)


if __name__ == "__main__":
    step = Step('test')

    step('one')
    # step('two')
    step('three')
    # step('four')

    step.finish()
