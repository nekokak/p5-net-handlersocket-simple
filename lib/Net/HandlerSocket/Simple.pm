package Net::HandlerSocket::Simple;
use strict;
use warnings;
use Net::HandlerSocket;
use Carp ();

our $VERSION = '0.01';

sub new {
    my ($class, $opts) = @_;

    for my $key (qw/host port/) {
        Carp::croak "Mandatory parameter $key missing" unless $opts->{$key};
    }

    bless +{
        ro_host       => $opts->{host},
        ro_port       => $opts->{port},
        ro_handle     => '',
        _ro_index     => 0,
        _ro_index_map => +{},
        wo_host       => $opts->{wo_host}||'',
        wo_port       => $opts->{wo_port}||'',
        wo_handle     => '',
        _wo_index     => 0,
        _wo_index_map => +{},
    }, $class;
}

sub ro_handle {
    my $self = shift;
    $self->{ro_handle} ||= Net::HandlerSocket->new(+{host => $self->{ro_host}, port => $self->{ro_port}});
}

sub wo_handle {
    my $self = shift;

    $self->{wo_handle} ||= do {
        for my $key (qw/wo_host wo_port/) {
            Carp::croak "Mandatory parameter $key missing" unless $self->{$key};
        }
        Net::HandlerSocket->new(+{host => $self->{wo_host}, port => $self->{wo_port}});
    };
}

sub insert {
    my ($self, $db, $args) = @_;

    my @fields = keys   %$args;
    my @values = values %$args;
    my $index = $self->_get_index('w', $db, '', \@fields);

    my $res = $self->wo_handle->execute_single($index, '+', \@values, 1, 0);
    if ($res->[0] != 0) {
        $self->handle_error($db, $res->[0], $self->wo_handle->get_error(), $args);
    }

    $res->[1];
}

sub bulk_insert {
    my ($self, $db, $args) = @_;

    my $base_row = $args->[0];
    my @fields = keys   %$base_row;
    my @values = values %$base_row;
    my $index = $self->_get_index('w', $db, '', \@fields);

    my @exec_args;
    for my $arg (@$args) {
        push @exec_args, [$index, '+', [values %$arg], 1, 0];
    }

    my $rv = $self->wo_handle->execute_multi(\@exec_args);
    for my $res (@$rv) {
        if ($res->[0] != 0) {
            $self->handle_error($db, $res->[0], $self->wo_handle->get_error(), $args);
        }
    }

    return;
};

sub update {
    my ($self, $db, $args) = @_;

    my @fields = keys   %{$args->{set}};
    my @values = values %{$args->{set}};
    my $index = $self->_get_index('w', $db, '', \@fields);

    my $res = $self->wo_handle->execute_single($index, '=', $args->{where}, 1, 0, 'U', \@values);
    if ($res->[0] != 0) {
        $self->handle_error($db, $res->[0], $self->wo_handle->get_error(), $args);
    }

    $res->[1];
}

sub delete {
    my ($self, $db, $args) = @_;

    my $index = $self->_get_index('w', $db, '', []);

    my $res = $self->wo_handle->execute_single($index, '=', $args->{where}, 1, 0, 'D');
    if ($res->[0] != 0) {
        $self->handle_error($db, $res->[0], $self->wo_handle->get_error(), $args);
    }

    $res->[1];
}

sub select {
    my ($self, $db, $args, $opts) = @_;

    my $slice = $opts->{slice} || 'flat';
    if ($slice !~ /^(hash|array|flat)$/) {
        Carp::croak 'unknown slice: ' . $slice;
    }

    my $index = $self->_get_index('r', $db, $opts->{'index'}, $args->{fields}, $opts->{filter});

    my $filters = $self->_make_filter_cond($opts->{filter});
    my ($where, $in_idx, $in_cond) = $self->_make_cond($args->{where});

    my $op     = $opts->{op}     || '=';
    my $limit  = $opts->{limit}  || (scalar(@{$in_cond||[]}) ? scalar(@{$in_cond||[]}) : 1);
    my $offset = $opts->{offset} || 0;

    my @exec_args = ($index, $op, $where, $limit, $offset);
    if ($filters) {
        push @exec_args, (undef, undef, $filters);
    }
    if ($in_cond) {
        push @exec_args, ($filters ? ($in_idx, $in_cond) : (undef,undef,undef, $in_idx, $in_cond));
    }
    
    my $res = $self->ro_handle->execute_find(@exec_args);
    if ($res->[0] != 0) {
        $self->handle_error($db, $res->[0], $res->[1], {%$args, %$opts});
    }
    shift @$res;

    return $res if $slice eq 'flat';
    return $self->_response_filter($res, $args->{fields}, $slice);
}

sub handle_error {
    my ($self, $db, $code, $reason, $args) = @_;

    Carp::croak sprintf <<"TRACE", $db, $code, $reason, Data::Dumper::Dumper($args);
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
@@@@@ Net::HandlerSocket::Simple 's Exception @@@@@
DB      : %s
Code    : %s
Reason  : %s
ARGS    : %s
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
TRACE
}

sub _get_index {
    my ($self, $mode, $db_table, $index, $fields, $filters) = @_;

    $index ||= 'PRIMARY';
    my ($db, $table) = split /\./, $db_table;
    my $fields_list  = (join ',', @$fields);
    my $filters_list = (join ',', keys %$filters);
    my $index_key = join '.', $db, $table, $index, $fields_list, $filters_list;

    my ($_index_map_key, $_index_key, $_handle) = $mode eq 'r' ? (qw/_ro_index_map _ro_index ro_handle/) : (qw/_wo_index_map _wo_index wo_handle/);

    $self->{$_index_map_key}->{$index_key} ||= do {
        my $i = ++$self->{$_index_key};
        my $rv = $self->$_handle->open_index($i, $db, $table, $index, $fields_list, $filters_list);
        if ($rv != 0) {
            $self->handle_error($db_table, $rv, $self->$_handle->get_error(), +{'index' => $index, fields => $fields_list, filters => $filters_list});
        }
        $i;
    };
}

sub _make_filter_cond {
    my ($self, $filter) = @_;

    my $filters;
    my $_filter_potition=0;
    my ($_filter_val, $_filter_op, $_filter_setting);
    for my $key (keys %$filter) {
        $_filter_setting = $filter->{$key};
        if (ref($_filter_setting) eq 'HASH') {
            ($_filter_op, $_filter_val) = each %$_filter_setting;
        }
        else {
            ($_filter_op, $_filter_val) = ('=', $_filter_setting);
        }
        push @$filters, ['F', $_filter_op, $_filter_potition, $_filter_val];
        $_filter_potition++;
    }
    $filters;
}

sub _make_cond {
    my ($self, $where) = @_;

    my ($in_idx, $in_cond);
    my $where_count = scalar(@$where);

    for (my $i=0 ; $i < $where_count ; $i++) {
        next if ref($where->[$i]) ne 'ARRAY';
        die 'HandlerSocket IN specific for only one column' if $in_idx;
        $in_idx  = $i;
        $in_cond = $where->[$i];
        $where->[$i] = '';
    }
    ($where, $in_idx, $in_cond);
}

sub _response_filter {
    my ($self, $dat, $fields, $slice) = @_;

    my (@result, @_tmp);
    my $fields_count = scalar(@$fields);
    while (@_tmp = splice(@$dat, 0, $fields_count)) {
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

=item * $options->{wo_host}

optional

handler socket daemon host for write.

=item * $options->{wo_port}

optional

handler socket daemon port for write.

=back

=head2 $hs->insert($db_table, \%row);

=over4

=item * $db_table

database name and table name that concat '.'

=item * %row

row data here.

=back

=head2 $hs->bulk_insert($db_table, \@rows);

=head2 $hs->update($db_table, \%cond);

=over

=item * $cond{where}

update conditon here.

=item * $cond{set}

update data here.

=back

=head2 $hs->delete($db_table, \%cond);

=over

=item * $cond{where}

update conditon here.

=back

=head2 $hs->select($db_table, \%args, \%opts);

=over

=item * $args{fields}

fetch fields name here.

=item * $args{where}

where condition here.

=item * $opts{index}

index name here.

=item * $opts{filter}

filter conditon here.

=item * $opts{slice}

get data format.

flat | hash | array

=item * $opts{limit}

get limit data size.

=item * $opts{offset}

get offset data size.

=back



=head1 AUTHOR

Atsushi Kobayashi E<lt>nekokak _at_ gmail _dot_ comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
