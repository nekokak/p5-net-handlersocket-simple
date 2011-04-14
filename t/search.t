use strict;
use warnings;
use Test::More;
use DBI;

use Net::HandlerSocket::Simple;

my $dbh = DBI->connect('dbi:mysql:test','root','');

$dbh->do(q{
    CREATE TABLE IF NOT EXISTS handler_socket_test (
        id        INT auto_increment,
        name      TEXT,
        PRIMARY KEY  (id)
    )
});
$dbh->do(q{TRUNCATE TABLE handler_socket_test});
$dbh->do(q{
    INSERT INTO handler_socket_test (name) values ('nekokak'),('zigorou'),('xaicron'),('hidek')
});

my $rows = $dbh->selectall_arrayref('SELECT * FROM handler_socket_test', +{ Slice => +{} });
ok $rows;
note explain $rows;

my $hs = Net::HandlerSocket::Simple->new(+{host => '127.0.0.1', port => 9998});

my $dat = $hs->select('test.handler_socket_test',
    +{
        fields  => [qw/id name/],
        where   => +[
            [qw/1 2 3/]
        ],
    },
);

ok $dat;

note explain $dat;

$dat = $hs->select('test.handler_socket_test',
    +{
        fields  => [qw/id name/],
        where   => +[
            [qw/1 2 3/]
        ],
    },
    {
        limit => 4,
    }
);

ok $dat;

note explain $dat;

$dat = $hs->select('test.handler_socket_test',
    +{
        fields  => [qw/id name/],
        where   => +[
            [qw/1 2 3/]
        ],
    },
    {
        limit => 4,
        slice => 'hash',
    }
);

ok $dat;

note explain $dat;

$dat = $hs->select('test.handler_socket_test',
    +{
        fields  => [qw/id name/],
        where   => +[
            [qw/1 2 3/]
        ],
    },
    {
        limit => 4,
        slice => 'array',
    }
);

ok $dat;

note explain $dat;

done_testing;

