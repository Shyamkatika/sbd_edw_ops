  {{
    config(
        tags=["edw_ops_view_rec"],
    )

}}
  {{ sbd_edw_main.SP_call('DYNAMICVIEWFORTABLE_BYNAME(@@CONSOLIDATED@@,@@NETWORK@@)') }}
