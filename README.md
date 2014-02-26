# NAME

Otogiri::Plugin::DeleteCascade - Otogiri Plugin for cascading delete by following FK columns

# SYNOPSIS

    use Otogiri;
    use Otogiri::Plugin;

    Otogiri->load_plugin('DeleteCascade');

    my $db = Otogiri->new( connect_info => $connect_info );
    $db->insert('parent_table', { id => 123, value => 'aaa' });
    $db->insert('child_table',  { parent_id => 123, value => 'bbb'}); # child.parent_id referes parent_table.id(FK)

    $db->delete_cascade('parent_table', { id => 123 }); # both parent_table and child_table are deleted.

# DESCRIPTION

Otogiri::Plugin::DeleteCascade is plugin for [Otogiri](http://search.cpan.org/perldoc?Otogiri) which provides cascading delete feature.
loading this plugin, `delete_cascade` method is exported. `delete_cascade` follows Foreign Keys(FK) and
delete data referred in these key.

# METHOD

## $self->delete\_cascade($table\_name, $cond\_href);

Delete rows that matched to $cond\_href and child table rows that can be followed by Foreign Keys.

# LICENSE

Copyright (C) Takuya Tsuchida.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

# AUTHOR

Takuya Tsuchida <tsucchi@cpan.org>
