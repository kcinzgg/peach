"""将运行时资源装入 wheel，源码树仍是资源的唯一维护位置。"""
from pathlib import Path
import runpy

from setuptools import setup
from setuptools.command.build_py import build_py


class BuildWithResources(build_py):
    def run(self):
        super().run()
        root = Path(__file__).resolve().parent
        destination = Path(self.build_lib) / "peach" / "_resources"
        for name in ("migrations", "web", "resources"):
            self.copy_tree(str(root / name), str(destination / name))
        bridge_data = runpy.run_path(str(root / 'scripts' / 'amane_bundle.py'))['bridge_data']
        for source, target in bridge_data(root):
            directory = destination / target
            self.mkpath(str(directory))
            self.copy_file(source, str(directory / Path(source).name))


setup(cmdclass={"build_py": BuildWithResources})
