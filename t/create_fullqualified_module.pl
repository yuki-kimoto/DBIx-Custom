use strict;
use warnings;

use FindBin;
use File::Basename qw/basename fileparse/;
use File::Copy 'copy';
use File::Path 'mkpath';

my $top = $FindBin::Bin;
my $common = "$top/common";
my $common_fullqualified = "$top/common_fullqualified";
mkdir $common_fullqualified unless -d $common_fullqualified;

my @modules = grep { -f $_ } glob("$common/*");
for my $module (@modules) {
    my $module_base = basename $module;
    copy $module, "$common_fullqualified/$module_base"
      or die "Can't move module file: $!";
}

my @dirs = grep { -d $_ } glob("$FindBin::Bin/common/*");
for my $dir (@dirs) {
    my $base_dir = basename $dir;
    my $model_dir_fullqualified = "$common_fullqualified/$base_dir";
    mkdir $model_dir_fullqualified unless -d $model_dir_fullqualified;
    
    my @files = grep { /table\d\.pm/ } glob("$dir/*");
    for my $file (@files) {
    
      my $content = do {
        open my $fh, '<', $file;
        local $/;
        <$fh>;
      };

      $content =~ s/::table(\d)/::main::table$1/g;
      $content =~ s/([^:])table(\d)/$1main.table$2/g;
      
      mkpath "$common_fullqualified/$base_dir/main";
      my $base_name = (fileparse($file, qr/\..+$/))[0];
      $base_name = $base_name;
      my $new_file = "$common_fullqualified/$base_dir/main/$base_name.pm";
      
      open my $fh, '>', $new_file
        or die "Can't write file: $!";
      
      print $fh $content;
    }
}
