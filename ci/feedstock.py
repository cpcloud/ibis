#!/usr/bin/env python

import os
import shutil
import sys

import click
import ruamel.yaml

from jinja2 import Environment, FileSystemLoader
from plumbum.cmd import git, conda

import ibis
from ibis.compat import Path, PY2


IBIS_DIR = Path(__file__).parent.parent.absolute()


def render(path):
    env = Environment(loader=FileSystemLoader(str(path.parent)))
    template = env.get_template(path.name)
    return template.render()


@click.group()
def cli():
    pass


default_repo = 'https://github.com/conda-forge/ibis-framework-feedstock'
default_dest = ibis.util.guid()
default_recipe_dir = os.path.join(default_dest, 'recipe')


def run(command):
    return command(
        stdout=click.get_binary_stream('stdout'),
        stderr=click.get_binary_stream('stderr')
    )


@cli.command()
@click.argument('repo-uri', default=default_repo)
@click.argument('destination', default=default_dest)
def clone(repo_uri, destination):
    if Path(destination).exists():
        return

    run(git['clone', repo_uri, destination])


@cli.command()
@click.argument('meta', default=os.path.join(default_recipe_dir, 'meta.yaml'))
@click.option('--source-path', default=str(IBIS_DIR))
def update(meta, source_path):
    path = Path(meta)

    click.echo('Updating {} recipe...'.format(path.parent))

    content = render(path)
    recipe = ruamel.yaml.round_trip_load(content)

    # update the necessary fields, skip leading 'v' in the version
    recipe['package']['version'] = ibis.__version__[1:]
    recipe['source'] = {'path': source_path}

    updated_content = ruamel.yaml.round_trip_dump(
        recipe, default_flow_style=False)

    if PY2:
        updated_content = updated_content.decode('utf-8')

    path.write_text(updated_content)


@cli.command()
@click.argument('recipe', default=os.path.join(default_dest, 'recipe'))
def build(recipe):
    click.echo('Building {} recipe...'.format(recipe))
    python_version = '{0.major}.{0.minor}.{0.micro}'.format(sys.version_info)
    run(conda['build', recipe,
              '--channel', 'conda-forge',
              '--python', python_version])


@cli.command()
@click.argument('package_location', type=click.Path(exists=True))
@click.argument('artifact_directory', type=click.Path(exists=False))
@click.argument('architectures', default=('noarch', 'linux-64', 'win-64'))
def deploy(package_location, artifact_directory, architectures):
    click.echo('!!!!!!Deploying PACKAGES!!!!!')
    artifact_dir = Path(artifact_directory)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    package_loc = Path(package_location)
    assert package_loc.exists(), 'Path {} does not exist'.format(package_loc)

    for architecture in architectures:
        arch_artifact_directory = artifact_dir / architecture
        arch_package_directory = package_loc / architecture
        if (arch_artifact_directory.exists() and
                arch_package_directory.exists()):
            click.echo(
                'Copying {} to {}'.format(
                    arch_package_directory, arch_artifact_directory))
            shutil.copytree(
                str(arch_package_directory), str(arch_artifact_directory))
    run(conda['index', artifact_directory])


@cli.command()
@click.pass_context
@click.option('--package-location', type=click.Path(exists=True))
@click.option('--artifact-directory', type=click.Path(exists=False))
def test(ctx, package_location, artifact_directory):
    ctx.invoke(clone)
    ctx.invoke(update)
    ctx.invoke(build)
    ctx.invoke(
        deploy,
        package_location=package_location,
        artifact_directory=artifact_directory)


if __name__ == '__main__':
    cli()
