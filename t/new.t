use strict;
use warnings;
use Test::More;
use Net::HandlerSocket::Simple;

eval {
    Net::HandlerSocket::Simple->new();
};
like $@, qr/Mandatory parameter host missing at/;

eval {
    Net::HandlerSocket::Simple->new(+{host => '127.0.0.1'});
};
like $@, qr/Mandatory parameter port missing at/;

eval {
    Net::HandlerSocket::Simple->new(+{port => 9998});
};
like $@, qr/Mandatory parameter host missing at/;

done_testing;

