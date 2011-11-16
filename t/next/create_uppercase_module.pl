use strict;
use warnings;

use FindBin;
use File::Basename qw/basename fileparse/;
use File::Copy 'copy';

my $top = $FindBin::Bin;
my $common = "$top/common";
my $common_uc = "$top/common_uc";
mkdir $common_uc unless -d $common_uc;

my @modules = grep { -f $_ } glob("$common/*");
for my $module (@modules) {
    my $module_base = basename $module;
    copy $module, "$common_uc/$module_base"
      or die "Can't move module file: $!";
}

my @dirs = grep { -d $_ } glob("$FindBin::Bin/common/*");
for my $dir (@dirs) {
    my $base_dir = basename $dir;
    my $model_dir_uc = "$common_uc/$base_dir";
    mkdir $model_dir_uc unless -d $model_dir_uc;
    
    my @files = grep { /table\d\.pm/ } glob("$dir/*");
    for my $file (@files) {
    
      my $content = do {
        open my $fh, '<', $file;
        local $/;
        <$fh>;
      };
      
      $content =~ s/table(\d)/TABLE$1/g;
      $content =~ s/TABLE2_alias/TABLE2_ALIAS/g;
      $content =~ s/key(\d)/KEY$1/g;
      
      my $base_name = (fileparse($file, qr/\..+$/))[0];
      $base_name = uc $base_name;
      my $new_file = "$common_uc/$base_dir/$base_name.pm";
      
      open my $fh, '>', $new_file
        or die "Can't write file: $!";
      
      print $fh $content;
    }
}
