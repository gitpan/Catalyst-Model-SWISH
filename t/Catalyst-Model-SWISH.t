
use Test::More tests => 1;
SKIP: {

    eval "use SWISH::API";
    if ($@) {
        skip "SWISH::API not installed", 1;
    } 
    use_ok('Catalyst::Model::SWISH');
}


