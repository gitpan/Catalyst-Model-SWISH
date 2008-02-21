use ExtUtils::MakeMaker;
WriteMakefile(
    NAME              => 'Catalyst::Model::SWISH',
    VERSION_FROM      => 'lib/Catalyst/Model/SWISH.pm', # finds $VERSION
    PREREQ_PM         => {
   
        'Catalyst'              => 5.7, 
        'SWISH::API::Object'    => 0.09,
        'NEXT'                  => 0.60,
        'Time::HiRes'           => 1.86,
        'Data::Pageset'         => 1.03,
        'Sort::SQL'             => 0.01,

    }, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/Catalyst/Model/SWISH.pm', # retrieve abstract from module
       AUTHOR         => 'Peter Karman <perl@peknet.com>') : ()),
);
