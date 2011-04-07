package Net::HandlerSocket::Simple;
use strict;
use warnings;
use Net::HandlerSocket;
use Carp ();

our $VERSION = '0.01';

sub new {
    my ($class, $opts) = @_;

    bless +{
        handler_socket => Net::HandlerSocket->new(+{host => $opts->{host}, port => $opts->{port}}),
        _index         => 0,
        _index_map     => +{},
    }, $class;
}

sub select {
    my ($self, %args) = @_;
    my ($db, $table, $index, $op, $fields, $where, $filter, $limit, $offset, $slice) =
        @args{qw/db table index op fields where filter limit offset slice/};

    Carp::croak '(db|table) must be require' if not $db || not $table;

    $slice ||= 'flat';

    if ($slice !~ /^(hash|array|flat)$/) {
        Carp::croak 'unknown slice: ' . $slice;
    }

    $op ||= '=';

    my $filter_list = join ',', keys %$filter;
    my $fields_list   = join ',', @$fields;

    my $index_key = join('.', $db, $table, $index, $fields_list, $filter_list);

    my $idx = $self->{_index_map}->{$index_key} ||= do {
        my $i = ++$self->{_index};
        $self->{_index_map}->{$index_key} = $i;
        $self->{handler_socket}->open_index($i, $db, $table, $index, $fields_list, $filter_list);
        $i;
        # check error
    };

    my $filters;
    {
        my $filter_count=0;
        for my $key (keys %$filter) {

            my ($filter_val, $filter_op);

            my $filter_setting = $filter->{$key};
            if (ref($filter_setting) eq 'HASH') {
                ($filter_op, $filter_val) = each %$filter_setting;
            }
            elsif (ref($filter_setting) eq 'SCALAR') {
                $filter_op = '=';
                $filter_val = $filter_setting;
            }
            else {
                Carp::croak 'not allowed filter data format';
            }
            push @$filters, ['F', $filter_op, $filter_count, $filter_val];
            $filter_count++;
        }
    }

    my ($in_idx, $in_cond);
    my $where_count = scalar(@$where);
    {
        for (my $i=0 ; $i < $where_count ; $i++) {
            next if ref($where->[$i]) ne 'ARRAY';
            die 'HandlerSocket IN specific for one column' if $in_idx;
            $in_idx  = $i;
            $in_cond = $where->[$i];
            $where->[$i] = '';
        }
    }

    my @exec_args = ($idx, $op, $where, $limit, $offset);
    if ($filters) {
        push @exec_args, (undef, undef, $filters);
    }
    if ($in_cond) {
        push @exec_args, ($filters ? ($in_idx, $in_cond) : (undef,undef,undef, $in_idx, $in_cond));
    }
    
    my $hoge = $self->{handler_socket}->execute_find(
        @exec_args
    );
    my $rows = $self->{handler_socket}->execute_find(@exec_args);
    # XXX: check error

    shift @$rows;

    return $rows if $slice eq 'flat';

    my (@result, @_tmp);
    my $fields_count = scalar(@$fields);
    while (@_tmp = splice(@$rows, 0, $fields_count)) {
        if ($slice eq 'hash') {
            my $i=0;
            my %_dat = map {$_ => $_tmp[$i++]} @$fields;
            push @result, \%_dat;
        } elsif ($slice eq 'array') {
            push @result, [@_tmp];
        }
    }
    \@result;
}

1;

__END__

=head1 NAME

Net::HandlerSocket::Simple - handler socket simple client

=head1 SYNOPSIS

    use Net::HandlerSocket::Simple;
    my $hs = Net::HandlerSocket::Simple->new(+{host => '127.0.0.1', port => 9998});

=head1 DESCRIPTION

Net::HandlerSocket::Simple is Net::HandlerSocekt simple wrapper

=head1 METHODS

=head2 my $hs = Net::HandlerSocket::Simple->new(\%options);

=over 4

=item * $options->{host}

handler socket daemon host.

=item * $options->{port}

handler socket daemon port.

=back

=head1 AUTHOR

Atsushi Kobayashi E<lt>nekokak _at_ gmail _dot_ comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
