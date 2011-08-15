use strict;
use warnings;

use FindBin;
use File::Basename 'fileparse';

my @dirs = grep { -d $_ } glob("$FindBin::Bin/common/*");
for my $dir (@dirs) {
    my @files = grep { /table\d\.pm/ } glob("$dir/*");
    for my $file (@files) {
    
      my $content = do {
        open my $fh, '<', $file;
        local $/;
        <$fh>;
      };
      
      $content =~ s/table(\d)/TABLE$1/g;
      
      my $base_name = (fileparse($file, qr/\..+$/))[0];
      $base_name = uc $base_name;
      my $new_file = "$dir/$base_name.pm";
      
      open my $fh, '>', $new_file
        or die "Can't write file: $!";
      
      print $fh $content;
    }
}
