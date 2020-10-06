#!/bin/bash

RELEASE=2016-10
DATASET=core-i18n
LANGS=${2:-af als am an ar arz ast az azb ba bar be bg bn bpy br bs bug ca ce ceb ckb cs cv cy da de el en eo es et eu fa fi fo fr fy ga gd gl gu he hi hr hsb ht hu hy ia id io is it ja jv ka kk kn ko ku ky la lb li lmo lt lv mg min mk ml mn mr mrj ms my mzn nah nap nds ne new nl nn no oc or os pa pl pms pnb pt qu ro ru sa sah scn sco sh si sk sl sq sr su sv sw ta te tg th tl tr tt uk ur uz vec vi vo wa war yi yo zh}
FILENAMES="article_categories skos_categories labels infobox_properties interlanguage_links"
EXT=.ttl.bz2

base=${1:-$(pwd)/dbpedia}
dir="$base/$RELEASE/$DATASET"

echo "Downloading $RELEASE release of $DATASET into $dir"
echo
echo "Languages: $LANGS"
echo "Filenames: $FILENAMES"
echo

for lang in $LANGS
do
	echo "==== Language $lang ===="
	lang_dir="$dir/$lang"

	file="$lang_dir/_checksums.md5"
	if [ ! -e "$file" ]
	then
		echo "fetching checksums"
		curl -s --create-dirs "http://downloads.dbpedia.org/$RELEASE/$DATASET/$lang/_checksums.md5" -o "$file.part"
		mv "$file.part" "$file"
	fi

	for filename in $FILENAMES
	do
		filename="${filename}_$lang$EXT"
		file="$lang_dir/$filename"
		if [ ! -e "$file" ]
		then
			echo "fetching $filename"
			curl -C - --progress-bar --create-dirs "http://downloads.dbpedia.org/$RELEASE/$DATASET/$lang/$filename" -o "$file.part"
			mv "$file.part" "$file"
		fi
	done

	echo "checking checksums"
	(cd "$base/$RELEASE"; md5sum -c --ignore-missing $DATASET/$lang/_checksums.md5)

	echo
done

