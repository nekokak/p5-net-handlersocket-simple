use strict;
use warnings;
use Test::More;
use DBI;

use Net::HandlerSocket::Simple;

my $dbh = DBI->connect('dbi:mysql:test','root','');

$dbh->do(q{
    CREATE TABLE IF NOT EXISTS handler_socket_test (
        id        INT auto_increment,
        name      VARCHAR(10),
        PRIMARY KEY  (id),
        KEY name_idx (name)
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

subtest 'slice => flat' => sub {
    my $dat = $hs->select('test.handler_socket_test',
        +{
            fields  => [qw/id name/],
            where   => [1],
        },
    );
    is_deeply $dat, [qw/1 nekokak/];
};

subtest 'slice => hash' => sub {
    my $dat = $hs->select('test.handler_socket_test',
        +{
            fields  => [qw/id name/],
            where   => [1],
        },
        +{
            slice => 'hash',
        }
    );
    is_deeply $dat, [+{ id => 1, name => 'nekokak'}];
};

subtest 'slice => array' => sub {
    my $dat = $hs->select('test.handler_socket_test',
        +{
            fields  => [qw/id name/],
            where   => [1],
        },
        +{
            slice => 'array',
        }
    );
    is_deeply $dat, [[qw/1 nekokak/],];
};

subtest 'get name only' => sub {
    my $dat = $hs->select('test.handler_socket_test',
        +{
            fields  => [qw/name/],
            where   => [1],
        },
    );
    is_deeply $dat, [qw/nekokak/];
};

subtest 'cond IN' => sub {
    my $dat = $hs->select('test.handler_socket_test',
        +{
            fields  => [qw/id name/],
            where   => [[qw/1 2 3/]],
        },
    );
    is_deeply $dat, [qw/1 nekokak 2 zigorou 3 xaicron/];
};

subtest 'cond IN and limit 2' => sub {
    my $dat = $hs->select('test.handler_socket_test',
        +{
            fields  => [qw/id name/],
            where   => [[qw/1 2 3/]],
        },
        +{
            limit => 2,
        }
    );
    is_deeply $dat, [qw/1 nekokak 2 zigorou/];
};

subtest 'cond IN and limit 2 offset 1' => sub {
    my $dat = $hs->select('test.handler_socket_test',
        +{
            fields  => [qw/id name/],
            where   => [[qw/1 2 3/]],
        },
        +{
            limit  => 2,
            offset => 1,
        }
    );
    is_deeply $dat, [qw/2 zigorou 3 xaicron/];
};

subtest 'filter' => sub {
    $dbh->do(q{
        INSERT INTO handler_socket_test (name) values ('nekokak'),('nekokak')
    });

    my $dat = $hs->select('test.handler_socket_test',
        +{
            fields  => [qw/id name/],
            where   => ['nekokak'],
        },
        +{
            index => 'name_idx',
            filter => +{
                id => 1,
            },
        }
    );
    is_deeply $dat, [qw/1 nekokak/];
};

done_testing;

