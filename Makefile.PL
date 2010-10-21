use ExtUtils::MakeMaker;
WriteMakefile
(
          'INSTALLDIRS' => 'site',
          'NAME' => 'DBIx::Custom',
          'EXE_FILES' => [],
          'VERSION_FROM' => 'lib/DBIx/Custom.pm',
          'PREREQ_PM' => {
                           'Test::More' => 0,
                           'Object::Simple' => '3.0201',
                           'DBD::SQLite' => '1.25',
                           'DBI' => '1.605'
                         }
        )
;
