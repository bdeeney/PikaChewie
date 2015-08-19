import os
import sys

####
##
## Project Specific Settings
##
####

project = 'PikaChewie'
intersphinx_mapping = {
    'python': ('http://docs.python.org/', None),
    'pika': ('https://pika.readthedocs.org/en/latest/', None),
}

##
## NOTE: Anything changed below this line should be changed in base_service.git
## and then merged into individual projects.  This prevents conflicts and
## maintains consistency between projects.
##

# If your extensions are in another directory, add it here. If the directory
# is relative to the documentation root, use os.path.abspath to make it
# absolute, like shown here.
sys.path.insert(0, os.path.abspath('.'))

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.intersphinx',
    'sphinx.ext.todo',
    'sphinx.ext.coverage',
    'sphinx.ext.ifconfig',
]

copyright = '2013-2015, Bryan D. Deeney'
pygments_style = 'sphinx'

templates_path = ['_templates']
exclude_trees = ['build']
html_static_path = ['_static']

html_theme = 'nature'

master_doc = 'index'
todo_include_todos = True
version = release = open('../../RELEASE-VERSION').readline().strip()

source_suffix = '.rst'
