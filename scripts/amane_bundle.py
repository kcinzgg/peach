"""发行包中的 amane 桥资源白名单，不包含解释器和上游源码。"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

BRIDGE_FILES = ('bridge.py', 'pyproject.toml', 'uv.lock')


def bridge_data(root: Path) -> list[tuple[str, str]]:
    directory = Path(root).resolve() / 'tools' / 'amane-bridge'
    files = [directory / name for name in BRIDGE_FILES]
    for path in files:
        if not path.is_file():
            raise FileNotFoundError(f'amane 发行资源缺失：{path}')
    return [(str(path), 'tools/amane-bridge') for path in files]


def arguments(root: Path) -> list[str]:
    return [argument for source, destination in bridge_data(root)
            for argument in ('--add-data', f'{source};{destination}')]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    print(json.dumps(arguments(parser.parse_args().root), ensure_ascii=False))
