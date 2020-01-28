#!/bin/sh
# postinst script for mica-search-es
#

set -e

# summary of how this script can be called:
#        * <postinst> `configure' <most-recently-configured-version>
#        * <old-postinst> `abort-upgrade' <new version>
#        * <conflictor's-postinst> `abort-remove' `in-favour' <package>
#          <new-version>
#        * <postinst> `abort-remove'
#        * <deconfigured's-postinst> `abort-deconfigure' `in-favour'
#          <failed-install-package> <version> `removing'
#          <conflicting-package> <version>
# for details, see http://www.debian.org/doc/debian-policy/ or
# the debian-policy package

NAME=mica-search-es

[ -r /etc/default/$NAME ] && . /etc/default/$NAME

case "$1" in
  [1-2])

      if [ -f /etc/default/mica2 ]; then
        . /etc/default/mica2
        mkdir -p $MICA_HOME/plugins
        if [ -d "$MICA_HOME"/plugins ]; then

          OLD_PLUGIN=$(ls -t "$MICA_HOME"/plugins/ | grep mica-search-es | head -1)
          NEW_PLUGIN=$(ls -t /usr/share/mica-search-es/ | grep mica-search-es | head -1 | sed s/\-dist\.zip//g)
          NEW_PLUGIN_ZIP="$NEW_PLUGIN-dist.zip"

          unzip /usr/share/mica-search-es/$NEW_PLUGIN_ZIP -d $MICA_HOME/plugins/
          touch $MICA_HOME/plugins/$NEW_PLUGIN

          if [ ! -z "$OLD_PLUGIN" ] && [ -f $MICA_HOME/plugins/$OLD_PLUGIN/site.properties ]; then
            echo "Copying $OLD_PLUGIN/site.properties to new installation."
            cp $MICA_HOME/plugins/$OLD_PLUGIN/site.properties $MICA_HOME/plugins/$NEW_PLUGIN/
          fi

          if [ ! -z "$OLD_PLUGIN" ] && [ -f $MICA_HOME/plugins/$OLD_PLUGIN/elasticsearch.yml ]; then
            echo "Copying $OLD_PLUGIN/elasticsearch.yml to new installation."
            cp $MICA_HOME/plugins/$OLD_PLUGIN/elasticsearch.yml $MICA_HOME/plugins/$NEW_PLUGIN/
          fi

          chown -R mica:adm $MICA_HOME/plugins/$NEW_PLUGIN
          echo '***'
          echo '*** IMPORTANT: Mica Search ES plugin has been installed, you must restart Mica server.'
          echo '***'
        fi
      fi

  ;;

  *)
    echo "postinst called with unknown argument \`$1'" >&2
    exit 1
  ;;
esac

exit 0
